# -*- coding: utf-8 -*-

import numpy as np
from PIL import Image
import logging
import logging.handlers
import datetime as dt
from six.moves.queue import Queue, Empty as queue_empty
import gc
import os.path
import time

try:
    import scipy.ndimage as ndi
except ImportError:
    ndi = None

from trollsift import compose
from satpy import Scene
from satpy.resample import get_area_def
from posttroll.listener import ListenerContainer
from posttroll.publisher import NoisyPublisher
from posttroll.message import Message

# These longitudinally valid ranges are mid-way points calculated from
# satellite locations assuming the given satellites are in use
LON_LIMITS = {'Meteosat-11': [-37.5, 20.75],
              'Meteosat-10': [-37.5, 20.75],
              'Meteosat-8': [20.75, 91.1],
              'Himawari-8': [91.1, -177.15],
              'GOES-15': [-177.15, -105.],
              'GOES-13': [-105., -37.5],
              'Meteosat-7': [41.5, 41.50001],  # placeholder
              'GOES-16': [-90., -90.0001]  # placeholder
              }


def calc_pixel_mask_limits(adef, lon_limits):
    """Calculate pixel intervals from longitude ranges."""
    # We'll assume global grid from -180 to 180 longitudes
    scale = 360. / adef.x_size  # degrees per pixel

    left_limit = int((lon_limits[0] + 180) / scale)
    right_limit = int((lon_limits[1] + 180) / scale)

    # Satellite data spans 180th meridian
    if right_limit < left_limit:
        return [[right_limit, left_limit]]
    else:
        return [[0, left_limit], [right_limit, adef.x_size]]


def read_satpy(fname):
    """Read image data using SatPy."""
    scn = Scene(reader='generic_image', filenames=[fname, ])
    scn.load(['image'])
    return scn['image']


def read_image(fname, adef, lon_limits=None):
    """Read image to numpy array"""

    if not os.path.exists(fname):
        logging.error("Image %s doesn't exist.", fname)
        return None
    try:
        img = read_satpy(fname)
    except ValueError:
        logging.error("Reading image %s failed, retrying once.", fname)
        time.sleep(5)
        try:
            img = read_satpy(fname)
        except ValueError:
            logging.error("Reading image failed again, skipping!")
            return None

    # Read the image into memory
    img = img.compute()

    # Get the masked area
    mask = np.isnan(img[0, :, :])

    # Mask overlapping areas away
    if lon_limits:
        for sat in lon_limits:
            if sat in fname:
                mask_limits = calc_pixel_mask_limits(adef,
                                                     lon_limits[sat])
                for lim in mask_limits:
                    mask[:, lim[0]:lim[1]] = True
                break

    # Mask the data
    for i in range(img.bands.size):
        band = np.array(img[i, :, :])
        band[mask] = np.nan
        img[i, :, :] = band

    # Set the area definition, if not already there (eg. PNG images)
    if 'area' not in img.attrs:
        img.attrs['area'] = adef

    return img


def create_world_composite(fnames, adef, sat_limits, img=None, logger=logging):
    """Create world composite from files *fnames*"""
    for fname in fnames:
        logger.info("Reading image %s", fname)
        next_img = read_image(fname, adef, sat_limits)

        if img is None:
            img = next_img
        elif next_img is None:
            continue
        else:
            logger.debug("Creating mask")
            img_mask = np.isnan(np.array(img[0, :, :]))
            next_img_mask = np.isnan(np.array(next_img[0, :, :]))

            chmask = np.logical_and(img_mask, next_img_mask)

            dtype = img.dtype
            chdata = np.zeros(img_mask.shape, dtype=dtype)

            for i in range(img.bands.size):
                band = np.array(img[i, :, :])
                next_band = np.array(next_img[i, :, :])
                logger.debug("Merging channel %d", i)

                # Be sure that that also overlapping data is updated
                img_mask[~next_img_mask & ~img_mask] = True
                chdata[img_mask] = next_band[img_mask]
                chdata[next_img_mask] = band[next_img_mask]

                chdata[chmask] = np.nan
                img[i, :, :] = chdata

            # chdata = np.max(np.dstack((img.channels[-1].data,
            #                            next_img.channels[-1].data)),
            #                 2)
            # img.channels[-1] = np.ma.masked_where(chmask, chdata)
            img.attrs['area'] = adef
    return img


class WorldCompositeDaemon(object):

    logger = logging.getLogger(__name__)
    publish_topic = "/global/mosaic/{areaname}"
    nameservers = None
    port = 0
    aliases = None
    broadcast_interval = 2

    def __init__(self, config):
        self.config = config
        self.slots = {}
        # Structure of self.slots is:
        # slots = {datetime(): {composite: {"img": None,
        #                              "num": 0},
        #                       "timeout": None}}
        self._parse_settings()
        self._listener = ListenerContainer(topics=config["topics"])
        self._set_message_settings()
        self._publisher = \
            NoisyPublisher("WorldCompositePublisher",
                           port=self.port,
                           aliases=self.aliases,
                           broadcast_interval=self.broadcast_interval,
                           nameservers=self.nameservers)
        self._publisher.start()
        self._loop = False
        if isinstance(config["area_def"], str):
            self.adef = get_area_def(config["area_def"])
        else:
            self.adef = config["area_def"]

    def run(self):
        """Listen to messages and make global composites"""
        self._loop = True

        while self._loop:
            if self._check_timeouts_and_save():
                num = gc.collect()
                self.logger.debug("%d objects garbage collected", num)

            # Get new messages from the listener
            msg = None
            try:
                msg = self._listener.output_queue.get(True, 1)
            except KeyboardInterrupt:
                self._loop = False
                break
            except queue_empty:
                continue

            if msg is not None and msg.type == "file":
                self._handle_message(msg)

        self._listener.stop()
        self._publisher.stop()

    def _set_message_settings(self):
        """Set message settings from config"""
        if "message_settings" not in self.config:
            return

        self.publish_topic = \
            self.config["message_settings"].get("publish_topic",
                                                "/global/mosaic/{areaname}")
        self.nameservers = \
            self.config["message_settings"].get("nameservers", None)
        self.port = self.config["message_settings"].get("port", 0)
        self.aliases = self.config["message_settings"].get("aliases", None)
        self.broadcast_interval = \
            self.config["message_settings"].get("broadcast_interval", 2)

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
        """Check timeouts, save completed images, and cleanup slots."""
        # Number of expected images
        num_expected = self.config["num_expected"]

        # Check timeouts and completed composites
        check_time = dt.datetime.utcnow()

        saved = False
        empty_slots = []
        slots = self.slots.copy()
        for slot in slots:
            composites = tuple(slots[slot].keys())
            for composite in composites:
                if (check_time > slots[slot][composite]["timeout"] or
                        slots[slot][composite]["num"] == num_expected):
                    fnames = slots[slot][composite]["fnames"]
                    self._create_global_mosaic(fnames, slot, composite)
                    saved = True

            # Collect empty slots
            if len(slots[slot]) == 0:
                empty_slots.append(slot)

        for slot in empty_slots:
            self.logger.debug("Removing empty time slot: %s",
                              str(slot))
            del self.slots[slot]

        return saved

    def _parse_settings(self):
        """Parse static settings from config"""
        lon_limits = LON_LIMITS.copy()
        try:
            lon_limits.update(self.config["lon_limits"])
        except KeyError:
            pass
        except TypeError:
            lon_limits = None
        self.config["lon_limits"] = lon_limits

        # Get image save options
        try:
            save_kwargs = self.config["save_settings"]
        except KeyError:
            save_kwargs = {}
        self.config["save_settings"] = save_kwargs


    def _create_global_mosaic(self, fnames, slot, composite):
        """Create and save global mosaic."""
        self.logger.info("Building composite %s for slot %s",
                         composite, str(slot))
        scn = Scene()
        file_parts = self._get_fname_parts(slot, composite)
        fname_out = file_parts["uri"]

        img = self._get_existing_image(fname_out)

        self.logger.info("Creating composite")
        scn['img'] = create_world_composite(fnames,
                                            self.adef,
                                            self.config["lon_limits"],
                                            img=img,
                                            logger=self.logger)
        self.logger.info("Saving %s", fname_out)
        scn.save_dataset('img', filename=fname_out,
                         **self.config["save_settings"])
        self._send_message(file_parts)
        del self.slots[slot][composite]

    def _get_fname_parts(self, slot, composite):
        """Get filename part dictionary"""
        file_parts = {'composite': composite,
                      'nominal_time': slot,
                      'areaname': self.adef.area_id}

        fname_out = compose(self.config["out_pattern"],
                            file_parts)
        file_parts['uri'] = fname_out
        file_parts['uid'] = os.path.basename(fname_out)

        return file_parts

    def _get_existing_image(self, fname_out):
        """Read an existing image and return it.  If the image doesn't exist,
        return None"""
        # Check if we already have an image with this filename
        if os.path.exists(fname_out):
            img = read_image(fname_out, self.adef.area_id)
            self.logger.info("Existing image was read: %s", fname_out)
        else:
            img = None

        return img

    def _send_message(self, file_parts):
        """Send a message"""
        msg = Message(compose(self.publish_topic, file_parts),
                      "file", file_parts)
        self._publisher.send(str(msg))
        self.logger.info("Sending message: %s", str(msg))

    def stop(self):
        """Stop"""
        self.logger.info("Stopping WorldCompositor")
        self._listener.stop()
        self._publisher.stop()

    def set_logger(self, logger):
        """Set logger."""
        self.logger = logger
