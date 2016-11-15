# -*- coding: utf-8 -*-

import numpy as np
from PIL import Image
import logging
import logging.handlers
import datetime as dt
import Queue

try:
    import scipy.ndimage as ndi
except ImportError:
    ndi = None

from trollsift import compose
from mpop.imageo.geo_image import GeoImage
from mpop.projector import get_area_def
from posttroll.listener import ListenerContainer

# These longitudinally valid ranges are mid-way points calculated from
# satellite locations assuming the given satellites are in use
LON_LIMITS = {'Meteosat-11': [-37.5, 20.75],
              'Meteosat-10': [-37.5, 20.75],
              'Meteosat-8': [20.75, 91.1],
              'Himawari-8': [91.1, -177.15],
              'GOES-15': [-177.15, -105.],
              'GOES-13': [-105., -37.5],
              'Meteosat-7': [41.5, 41.50001],  # placeholder
              'GOES-R': [-90., -90.0001]  # placeholder
              }


def calc_pixel_mask_limits(adef, lon_limits):
    """Calculate pixel intervals from longitude ranges."""
    # We'll assume global grid from -180 to 180 longitudes
    scale = 360. / adef.shape[1]  # degrees per pixel

    left_limit = int((lon_limits[0] + 180) / scale)
    right_limit = int((lon_limits[1] + 180) / scale)

    # Satellite data spans 180th meridian
    if right_limit < left_limit:
        return [[right_limit, left_limit]]
    else:
        return [[0, left_limit], [right_limit, adef.shape[1]]]


def read_image(fname, tslot, adef, lon_limits=None):
    """Read image to numpy array"""
    # Convert to float32 to save memory in later steps
    try:
        img = np.array(Image.open(fname)).astype(np.float32)
    except IOError:
        return None

    mask = img[:, :, 3]

    # Mask overlapping areas away
    if lon_limits:
        for sat in lon_limits:
            if sat in fname:
                mask_limits = calc_pixel_mask_limits(adef, lon_limits[sat])
                for lim in mask_limits:
                    mask[:, lim[0]:lim[1]] = 0
                break

    mask = mask == 0

    chans = []
    for i in range(4):
        chans.append(np.ma.masked_where(mask, img[:, :, i] / 255.))

    return GeoImage(chans, adef, tslot, fill_value=None, mode="RGBA",
                    crange=((0, 1), (0, 1), (0, 1), (0, 1)))


def create_world_composite(fnames, tslot, adef, sat_limits,
                           blend=None, img=None):
    """Create world composite from files *fnames*"""
    for fname in fnames:
        next_img = read_image(fname, tslot, adef, sat_limits)

        if img is None:
            img = next_img
        else:
            img_mask = reduce(np.ma.mask_or,
                              [chn.mask for chn in img.channels])
            next_img_mask = reduce(np.ma.mask_or,
                                   [chn.mask for chn in next_img.channels])

            chmask = np.logical_and(img_mask, next_img_mask)

            if blend and ndi:
                scaled_erosion_size = \
                    blend["erosion_width"] * (float(img.width) / 1000.0)
                scaled_smooth_width = \
                    blend["smooth_width"] * (float(img.width) / 1000.0)
                alpha = np.ones(next_img_mask.shape, dtype='float')
                alpha[next_img_mask] = 0.0
                smooth_alpha = ndi.uniform_filter(
                    ndi.grey_erosion(alpha, size=(scaled_erosion_size,
                                                  scaled_erosion_size)),
                    scaled_smooth_width)
                smooth_alpha[img_mask] = alpha[img_mask]

            dtype = img.channels[0].dtype
            chdata = np.zeros(img_mask.shape, dtype=dtype)

            for i in range(3):
                if blend and ndi:
                    if blend["scale"]:
                        chmask2 = np.invert(chmask)
                        idxs = img.channels[i] == 0
                        chmask2[idxs] = False
                        if np.sum(chmask2) == 0:
                            scaling = 1.0
                        else:
                            scaling = \
                                np.nanmean(next_img.channels[i][chmask2]) / \
                                np.nanmean(img.channels[i][chmask2])
                            if not np.isfinite(scaling):
                                scaling = 1.0
                        if scaling == 0.0:
                            scaling = 1.0
                    else:
                        scaling = 1.0

                    chdata = \
                        next_img.channels[i].data * smooth_alpha / scaling + \
                        img.channels[i].data * (1 - smooth_alpha)
                else:
                    # Be sure that that also overlapping data is updated
                    img_mask[~next_img_mask & ~img_mask] = True
                    chdata[img_mask] = next_img.channels[i].data[img_mask]
                    chdata[next_img_mask] = img.channels[i].data[next_img_mask]

                img.channels[i] = np.ma.masked_where(chmask, chdata)

            chdata = np.max(np.dstack((img.channels[3].data,
                                       next_img.channels[3].data)),
                            2)
            img.channels[3] = np.ma.masked_where(chmask, chdata)

    return img


class WorldCompositeDaemon(object):

    logger = logging.getLogger(__name__)

    def __init__(self, config):
        self.config = config
        self.slots = {}
        # slots = {tslot: {composite: {"img": None,
        #                              "num": 0},
        #                  "timeout": None}}

        self._listener = ListenerContainer(topics=config["topics"])
        self._loop = False
        if isinstance(config["area_def"], str):
            self.adef = get_area_def(config["area_def"])
        else:
            self.adef = config["area_def"]

    def run(self):
        """Listen to messages and make global composites"""
        self._loop = True

        while self._loop:
            self._check_timeouts_and_save()

            # Get new messages from the listener
            msg = None
            try:
                msg = self._listener.output_queue.get(True, 1)
            except KeyboardInterrupt:
                self._listener.stop()
                return
            except Queue.Empty:
                continue

            if msg.type == "file":
                self._handle_message(msg)

    def _handle_message(self, msg):
        """Insert file from the message to correct time slot and composite"""
        # Check which time should be used as basis for timeout:
        # - "message" = time of message sending
        # - "nominal_time" = time of satellite data, read from message data
        # - "receive" = current time when message is read from queue
        # Default to use slot nominal time
        timeout_epoch = self.config.get("timeout_epoch", "nominal_time")

        self.logger.debug("New message received: %s", str(msg.data))
        fname = msg.data["uri"]
        tslot = msg.data["nominal_time"]
        composite = msg.data["productname"]
        if tslot not in self.slots:
            self.slots[tslot] = {}
            self.logger.debug("Adding new timeslot: %s", str(tslot))
        if composite not in self.slots[tslot]:
            if timeout_epoch == "message":
                epoch = msg.time
            elif timeout_epoch == "receive":
                epoch = dt.datetime.utcnow()
            else:
                epoch = tslot
            self.slots[tslot][composite] = \
                {"fnames": [], "num": 0,
                 "timeout": epoch +
                 dt.timedelta(minutes=self.config["timeout"])}
            self.logger.debug("Adding new composite to slot %s: %s",
                              str(tslot), composite)
        self.logger.debug("Adding file to slot %s/%s: %s",
                          str(tslot), composite, fname)
        self.slots[tslot][composite]["fnames"].append(fname)
        self.slots[tslot][composite]["num"] += 1

    def _check_timeouts_and_save(self):
        """Check timeouts, save completed images and cleanup slots."""
        # Number of expected images
        num_expected = self.config["num_expected"]

        lon_limits = LON_LIMITS.copy()
        try:
            lon_limits.update(self.config["lon_limits"])
        except KeyError:
            pass
        except TypeError:
            lon_limits = None

        try:
            blend = self.config["blend_settings"]
        except KeyError:
            blend = None

        # Get image save options
        try:
            compression = self.config["save_settings"].get('compression', 6)
            tags = self.config["save_settings"].get('tags', None)
            fformat = self.config["save_settings"].get('fformat', None)
            gdal_options = self.config["save_settings"].get('gdal_options',
                                                            None)
            blocksize = self.config["save_settings"].get('blocksize', 0)
        except KeyError:
            compression = 6
            tags = None
            fformat = None
            gdal_options = None
            blocksize = 0

        # Check timeouts and completed composites
        check_time = dt.datetime.utcnow()

        empty_slots = []
        for slot in self.slots:
            for composite in self.slots[slot].keys():
                if (check_time > self.slots[slot][composite]["timeout"] or
                        self.slots[slot][composite]["num"] == num_expected):
                    file_parts = {'composite': composite,
                                  'nominal_time': slot,
                                  'areaname': self.adef.area_id}
                    self.logger.info("Building composite %s for slot %s",
                                     composite, str(slot))
                    fnames = self.slots[slot][composite]["fnames"]
                    fname_out = compose(self.config["out_pattern"],
                                        file_parts)
                    # Check if we already have an image with this filename
                    img = read_image(fname_out, slot,
                                     self.adef.area_id)
                    if img:
                        self.logger.info("Read existing image: %s", fname_out)

                    img = create_world_composite(fnames,
                                                 slot,
                                                 self.adef,
                                                 lon_limits,
                                                 blend=blend, img=img)
                    self.logger.info("Saving %s", fname_out)
                    img.save(fname_out, compression=compression,
                             tags=tags, fformat=fformat,
                             gdal_options=gdal_options,
                             blocksize=blocksize)
                    del self.slots[slot][composite]
                    del img
                    img = None

            # Collect empty slots
            if len(self.slots[slot]) == 0:
                empty_slots.append(slot)

        for slot in empty_slots:
            self.logger.debug("Removing empty time slot: %s",
                              str(slot))
            del self.slots[slot]

    def stop(self):
        """Stop"""
        self.logger.info("Stopping WorldCompositor")
        self._listener.stop()

    def set_logger(self, logger):
        """Set logger."""
        self.logger = logger
