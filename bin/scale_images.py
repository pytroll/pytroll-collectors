#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2015

# Author(s):

#   Panu Lahtinen <panu.lahtinen@fmi.fi>

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

'''Listen to messages and generate smaller versions of the images.

Example scale_images.ini:

[/HRPT/L3/dev/hki/euron1]
# <x_size>x<y_size>+<x_start>+<y_start>
crops = ,768x768+800+300,
sizes = 3072x3072,768x768,384x384
tags = large,small,thumb
out_dir = /tmp
areaname = euron1
in_pattern = {time:%Y%m%d_%H%M}_Metop-A_{areaname}_{composite}.png
out_dir = /lustre/tmp/data/oper/test/qlook/{tag}
out_pattern = {time:%Y%m%d_%H%M}_MetopA-{composite}-{areaname}.png
overlay_config = /home/users/satman/config_files/pycoast_euron1_overlay_config.ini
use_platform_name_hack = False
timeliness = 10
latest_composite_image = /tmp/latest_polar_data-{composite}-{areaname}.png
'''

import sys
import Queue
import os
import os.path
from ConfigParser import ConfigParser, NoOptionError
import logging
import logging.config
import datetime as dt
import glob
import time
from urlparse import urlparse

import numpy as np
from PIL import Image

from posttroll.listener import ListenerContainer
from pycoast import ContourWriter
from trollsift import parse, compose
from mpop.projector import get_area_def
from mpop.imageo.geo_image import GeoImage

GSHHS_DATA_ROOT = os.environ['GSHHS_DATA_ROOT']

LOG_CONFIG = {'version': 1,
              'handlers': {
                  'console':  {
                      'class': 'logging.StreamHandler',
                      'level': 'DEBUG',
                      'formatter': 'simple',
                      'stream': sys.stdout,
                  },
                  'file': {
                      'class': 'logging.handlers.TimedRotatingFileHandler',
                      'level': 'DEBUG',
                      'formatter': 'simple',
                      'filename': '/tmp/scale_images.log',
                      'backupCount': 7,
                      'when': 'midnight',
                      'utc': True,
                  }
              },
              'formatters': {
                  'simple': {
                      'format': '[%(levelname)s: %(asctime)s] %(message)s',
                  }
              },
              'loggers': {
                  '': {
                      'handlers': ['console', 'file'],
                      'level': 'DEBUG',
                      'propagate': True
                  }
              }
              }


class ImageScaler(object):

    '''Class for scaling images to defined sizes.'''

    def __init__(self, config):
        self.config = config
        topics = config.sections()
        self.listener = ListenerContainer(topics=topics)
        self._loop = True
        self._overlays = {}
        self._cw = ContourWriter(GSHHS_DATA_ROOT)

    def stop(self):
        '''Stop scaler before shutting down.'''
        if self._loop:
            self._loop = False
            if self.listener is not None:
                self.listener.stop()

    def run(self):
        '''Start waiting for messages.

        On message arrival, read the image, scale down to the defined
        sizes and add coastlines.
        '''

        while self._loop:
            # Wait for new messages
            try:
                msg = self.listener.queue.get(True, 5)
            except KeyboardInterrupt:
                self.stop()
                raise
            except Queue.Empty:
                continue

            logging.info("New message with topic %s", msg.subject)

            filepath = urlparse(msg.data["uri"]).path

            try:
                out_dir = self.config.get(msg.subject, 'out_dir')
            except NoOptionError:
                logging.debug("No config for %s", msg.subject)
                continue

            try:
                update_current = self.config.getboolean(msg.subject,
                                                        'update_current')
            except NoOptionError:
                logging.debug("No option 'update_current' given, "
                              "default to False")
                update_current = False

            try:
                is_backup = self.config.getboolean(msg.subject,
                                                   'only_backup')
            except NoOptionError:
                logging.debug("No option 'only_backup' given, "
                              "default to False")
                is_backup = False

            # Collect crop information
            crops = []
            try:
                crop_conf = self.config.get(msg.subject, 'crops').split(',')
            except NoOptionError:
                pass

            for crop in crop_conf:
                if 'x' in crop and '+' in crop:
                    # Crop strings are formated like this:
                    # <x_size>x<y_size>+<x_start>+<y_start>
                    # eg. 1000x300+103+200
                    # Origin (0, 0) is at top-left
                    parts = crop.split('+')
                    crop = tuple(map(int, parts[1:]) +
                                 map(int, parts[0].split('x')))

                    crops.append(crop)
                else:
                    crops.append(None)

            # Read the requested sizes from configuration section
            # named like the message topic
            sizes = []
            for size in self.config.get(msg.subject, 'sizes').split(','):
                sizes.append(map(int, size.split('x')))

            tags = [tag for tag in self.config.get(msg.subject,
                                                   'tags').split(',')]
            # get timeliness from config, if available
            try:
                timeliness = self.config.getint(msg.subject, 'timeliness')
            except NoOptionError:
                logging.debug("No timeliness given, using default of 10 min")
                timeliness = 10

            try:
                latest_composite_image = \
                    self.config.get(msg.subject, "latest_composite_image")
            except NoOptionError:
                latest_composite_image = None

            # get areaname from config
            areaname = self.config.get(msg.subject, 'areaname')

            # get the input file pattern and replace areaname
            in_pattern = self.config.get(msg.subject, 'in_pattern')
            in_pattern = in_pattern.replace('{areaname}', areaname)
            # parse filename parts from the incoming file
            try:
                fileparts = parse(in_pattern, os.path.basename(filepath))
            except ValueError:
                logging.info("Filepattern doesn't match, skipping.")
                continue
            fileparts['areaname'] = areaname

            try:
                use_hack = self.config.getboolean(msg.subject,
                                                  'use_platform_name_hack')
            except NoOptionError:
                use_hack = False
            if use_hack:
                # remove "-" from platform names
                fileparts['platform_name'] = fileparts[
                    'platform_name'].replace('-', '')

            # Check if there's a composite_stack to be updated

            # form the output filename
            out_pattern = self.config.get(msg.subject, 'out_pattern')
            out_pattern = os.path.join(out_dir, out_pattern)

            # Read overlay text settings
            try:
                text = self.config.get(msg.subject, 'text')
                text = compose(text, fileparts)
                text_settings = _get_text_settings(self.config, msg.subject)
            except NoOptionError:
                text = None

            # check if something silmiar has already been made:
            # checks for: platform_name, areaname and
            # start_time +- timeliness minutes
            check_start_time = msg.data["start_time"] - \
                dt.timedelta(minutes=timeliness)
            check_dict = fileparts.copy()
            check_dict["tag"] = tags[0]
            if is_backup:
                check_dict["platform_name"] = '*'
                check_dict["sat_loc"] = '*'
            check_dict["composite"] = '*'

            first_overpass = True
            update_fname_parts = None
            for i in range(2 * timeliness + 1):
                check_dict['time'] = check_start_time + dt.timedelta(minutes=i)
                glob_pattern = compose(os.path.join(out_dir, out_pattern),
                                       check_dict)
                glob_fnames = glob.glob(glob_pattern)
                if len(glob_fnames) > 0:
                    first_overpass = False
                    logging.debug("Found files: %s", str(glob_fnames))
                    try:
                        update_fname_parts = parse(out_pattern, glob_fnames[0])
                        update_fname_parts[
                            "composite"] = fileparts["composite"]
                        if not is_backup:
                            try:
                                update_fname_parts[
                                    "platform_name"] = fileparts["platform_name"]
                            except KeyError:
                                pass
                        break
                    except ValueError:
                        logging.debug("Parsing failed for update_fname_parts.")
                        logging.debug("out_pattern: %s, basename: %s",
                                      out_pattern, glob_fnames[0])
                        update_fname_parts = None

            # if not first_overpass:
            #     logging.info("Similar file already present, skipping.")
            #     continue

            if is_backup and not first_overpass:
                logging.info("File already exists, no backuping needed.")
                continue

            # area definition for coastlines
            area_def = get_area_def(self.config.get(msg.subject,
                                                    'areaname'))
            overlay_config = self.config.get(msg.subject, 'overlay_config')
            # area_def = (area_def.proj4_string, area_def.area_extent)

            # Read the image
            if msg.data["type"] == "PNG":
                img = Image.open(filepath)
                logging.info("Adding overlays")

                if msg.subject not in self._overlays:
                    logging.debug("Generating overlay")
                    self._overlays[msg.subject] = \
                        self._cw.add_overlay_from_config(overlay_config,
                                                         area_def)
                else:
                    logging.debug("Using overlay from cache")

                img.paste(self._overlays[msg.subject],
                          mask=self._overlays[msg.subject])

                for i in range(len(sizes)):
                    img_wrk = None
                    fileparts['tag'] = tags[i]

                    # Crop the image
                    try:
                        if crops[i] is not None:
                            img_wrk = img.crop(crops[i])
                        else:
                            img_wrk = img
                    except IndexError:
                        img_wrk = img

                    # Resize the image
                    x_res, y_res = sizes[i]

                    if img_wrk.size[0] == x_res and img_wrk.size[1] == y_res:
                        img_out = img_wrk
                    else:
                        img_out = img_wrk.resize((x_res, y_res))

                    fname = compose(out_pattern, fileparts)
                    if update_fname_parts is not None:
                        update_fname_parts['tag'] = tags[i]
                        if update_current:
                            fname = compose(os.path.join(out_dir, out_pattern),
                                            update_fname_parts)
                            logging.info("Updating image %s with image %s",
                                         fname, filepath)
                            img_out = update_latest_composite_image(fname,
                                                                    img_out)
                            if text is not None:
                                img_out = add_text(
                                    img_out, text, text_settings)
                            img_out.save(fname)
                            logging.info("Saving image %s with resolution "
                                         "%d x %d", fname, x_res, y_res)
                    else:
                        if text is not None:
                            img_out = add_text(img_out, text, text_settings)
                        img_out.save(fname)
                        logging.info("Saving image %s with resolution "
                                     "%d x %d", fname, x_res, y_res)

                    # Update latest composite image, if given in config
                    if latest_composite_image:
                        try:
                            fname = \
                                compose(os.path.join(out_dir,
                                                     latest_composite_image),
                                        fileparts)
                            img_out = update_latest_composite_image(fname,
                                                                    img_out)
                            if text is not None:
                                img_out = add_text(
                                    img_out, text, text_settings)
                            img_out.save(fname)
                            logging.info("Updated latest composite image %s",
                                         fname)
                        except Exception as err:
                            logging.error("Update of 'latest' %s failed: %s",
                                          fname, str(err))


def _get_text_settings(config, subject):
    """Parse text settings from the config."""
    stgs = {}
    try:
        stgs['loc'] = config.get(subject, 'text_location')
    except NoOptionError:
        stgs['loc'] = 'SW'

    try:
        stgs['font_fname'] = config.get(subject, 'font')
    except NoOptionError:
        stgs['font_fname'] = None

    try:
        stgs['font_size'] = config.getint(subject, 'font_size')
    except NoOptionError:
        stgs['font_size'] = 12

    try:
        stgs['text_color'] = [int(x) for x in
                              config.get(subject,
                                         'text_color').split(',')]
    except NoOptionError:
        stgs['text_color'] = [0, 0, 0]

    try:
        stgs['bg_color'] = [int(x) for x in
                            config.get(subject,
                                       'text_bg_color').split(',')]
    except NoOptionError:
        stgs['bg_color'] = [255, 255, 255]

    try:
        stgs['x_marginal'] = config.getint(subject, 'x_marginal')
    except NoOptionError:
        stgs['x_marginal'] = 10

    try:
        stgs['y_marginal'] = config.getint(subject, 'y_marginal')
    except NoOptionError:
        stgs['y_marginal'] = 3

    try:
        stgs['bg_extra_width'] = config.getint(subject,
                                               'bg_extra_width')
    except (ValueError, NoOptionError):
        stgs['bg_extra_width'] = None

    return stgs


def add_text(img, text, settings):
    """Add text to the image"""
    from PIL import ImageDraw, ImageFont

    if 'L' in img.mode:
        mode = 'RGB'
        if 'A' in img.mode:
            mode += 'A'
        logging.info("Converting to %s", mode)
        img = img.convert(mode)

    width, height = img.size
    draw = ImageDraw.Draw(img)
    try:
        font = ImageFont.truetype(settings['font_fname'],
                                  settings['font_size'])
        logging.debug('Font read from %s', settings['font_fname'])
    except (IOError, TypeError):
        try:
            font = ImageFont.load(settings['font_fname'])
            logging.debug('Font read from %s', settings['font_fname'])
        except (IOError, TypeError):
            logging.warning('Falling back to default font')
            font = ImageFont.load_default()

    textsize = draw.textsize(text, font)

    x_marginal = settings['x_marginal']
    y_marginal = settings['y_marginal']
    bg_extra_width = settings['bg_extra_width']

    if 'S' in settings['loc']:
        if 'W' in settings['loc']:
            text_loc = (x_marginal, height - textsize[1] - 2 * y_marginal)
            if bg_extra_width is not None:
                box_loc = [text_loc[0] - bg_extra_width,
                           height - textsize[1] - 2 * y_marginal,
                           text_loc[0] + textsize[0] + bg_extra_width,
                           height]
            else:
                box_loc = [0, height - textsize[1] - 2 * y_marginal,
                           width, height]
        elif 'E' in settings['loc']:
            text_loc = (width - textsize[0] - x_marginal,
                        height - textsize[1] - 2 * y_marginal)
            if bg_extra_width is not None:
                box_loc = [text_loc[0] - bg_extra_width,
                           height - textsize[1] - 2 * y_marginal,
                           text_loc[0] + textsize[0] + bg_extra_width,
                           height]
            else:
                box_loc = [0, height - textsize[1] - 2 * y_marginal,
                           width, height]
        # Center
        else:
            text_loc = ((width - textsize[0]) / 2,
                        height - textsize[1] - 2 * y_marginal)
            if bg_extra_width is not None:
                box_loc = [text_loc[0] - bg_extra_width,
                           height - textsize[1] - 2 * y_marginal,
                           text_loc[0] + textsize[0] + bg_extra_width,
                           height]
            else:
                box_loc = [0, height - textsize[1] - 2 * y_marginal,
                           width, height]
    else:
        if 'W' in settings['loc']:
            text_loc = (x_marginal, y_marginal)
            if bg_extra_width is not None:
                box_loc = [text_loc[0] - bg_extra_width,
                           0,
                           text_loc[0] + textsize[0] + bg_extra_width,
                           textsize[1] + 2 * y_marginal]
            else:
                box_loc = [0, 0, width, textsize[1] + 2 * y_marginal]
        elif 'E' in settings['loc']:
            text_loc = (width - textsize[0] - x_marginal, 0)  # y_marginal)
            if bg_extra_width is not None:
                box_loc = [text_loc[0] - bg_extra_width,
                           0,
                           text_loc[0] + textsize[0] + bg_extra_width,
                           textsize[1] + 2 * y_marginal]
            else:
                box_loc = [0, 0, width, textsize[1] + 2 * y_marginal]
        # Center
        else:
            text_loc = ((width - textsize[0]) / 2, 0)  # y_marginal)
            if bg_extra_width is not None:
                box_loc = [text_loc[0] - bg_extra_width,
                           0,
                           text_loc[0] + textsize[0] + bg_extra_width,
                           textsize[1] + 2 * y_marginal]
            else:
                box_loc = [0, 0, width, textsize[1] + 2 * y_marginal]

    draw.rectangle(box_loc, fill=tuple(settings['bg_color']))
    draw.text(text_loc, text, fill=tuple(settings['text_color']),
              font=font)

    return img


def update_latest_composite_image(fname, new_img):
    '''Read image from fname, if present, and update valid data (= not
    black) from img_in.  Return updated image as PIL image.
    '''

    new_img_mode = new_img.mode
    try:
        old_img = Image.open(fname)
    except IOError:
        return new_img

    if new_img_mode == 'LA':
        old_img = np.array(old_img.convert('RGBA'))
        old_img = np.dstack((old_img[:, :, 0], old_img[:, :, -1]))
        new_img = np.array(new_img.convert('RGBA'))
        new_img = np.dstack((new_img[:, :, 0], new_img[:, :, -1]))
    else:
        old_img = np.array(old_img.convert(new_img_mode))
        new_img = np.array(new_img)

    ndims = old_img.shape
    logging.debug("Image dimensions: old_img: %s, new_img: %s", str(ndims),
                  str(new_img.shape))
    if len(ndims) > 1:
        mask = np.max(new_img, -1) > 0
        for i in range(ndims[-1]):
            old_img[mask, i] = new_img[mask, i]
    else:
        mask = new_img > 0
        old_img[mask] = new_img[mask]

    return Image.fromarray(old_img, mode=new_img_mode)


def main():
    '''Main'''

    os.environ["TZ"] = "UTC"
    time.tzset()

    config_file = sys.argv[1]
    config = ConfigParser()
    config.read(config_file)

    logging.config.dictConfig(LOG_CONFIG)

    logging.info("Config read")

    scaler = ImageScaler(config)

    try:
        logging.info("Starting ImageScaler")
        scaler.run()
    except KeyboardInterrupt:
        logging.info("Stopping ImageScaler")
        scaler.stop()


if __name__ == "__main__":
    main()
