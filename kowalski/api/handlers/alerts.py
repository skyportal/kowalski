import gzip
import io
import pathlib
import traceback

import matplotlib.pyplot as plt
import numpy as np
from astropy.io import fits
from astropy.visualization import (
    AsinhStretch,
    AsymmetricPercentileInterval,
    ImageNormalize,
    LinearStretch,
    LogStretch,
    MinMaxInterval,
    SqrtStretch,
    ZScaleInterval,
)
from aiohttp import web
from bson.json_util import dumps, loads
from multidict import MultiDict

from kowalski.config import load_config
from kowalski.log import log
from kowalski.api.middlewares import (
    auth_required,
)
from .base import BaseHandler

""" load config and secrets """
config = load_config(config_files=["config.yaml"])["kowalski"]


class ZTFAlertCutouts(BaseHandler):
    # @routes.get('/lab/ztf-alerts/{candid}/cutout/{cutout}/{file_format}', allow_head=False)
    @auth_required
    async def get(self, request: web.Request) -> web.Response:
        """
        Serve ZTF alert cutouts as fits or png

        :param request:
        :return:

        ---
        summary: Serve ZTF alert cutout as fits or png
        tags:
          - lab

        parameters:
          - in: path
            name: candid
            description: "ZTF alert candid"
            required: true
            schema:
              type: integer
          - in: path
            name: cutout
            description: "retrieve science, template, or difference cutout image?"
            required: true
            schema:
              type: string
              enum: [science, template, difference]
          - in: path
            name: file_format
            description: "response file format: original lossless FITS or rendered png"
            required: true
            schema:
              type: string
              enum: [fits, png]
          - in: query
            name: interval
            description: "Interval to use when rendering png"
            required: false
            schema:
              type: string
              enum: [min_max, zscale]
          - in: query
            name: stretch
            description: "Stretch to use when rendering png"
            required: false
            schema:
              type: string
              enum: [linear,  asinh, sqrt]
          - in: query
            name: cmap
            description: "Color map to use when rendering png"
            required: false
            schema:
              type: string
              enum: [bone, gray, cividis, viridis, magma]

        responses:
          '200':
            description: retrieved cutout
            content:
              image/fits:
                schema:
                  type: string
                  format: binary
              image/png:
                schema:
                  type: string
                  format: binary

          '400':
            description: retrieval failed
            content:
              application/json:
                schema:
                  type: object
                  required:
                    - status
                    - message
                  properties:
                    status:
                      type: string
                      enum: [error]
                    message:
                      type: string
                example:
                  status: error
                  message: "failure: <error message>"
        """
        try:
            candid = int(request.match_info["candid"])
            cutout = request.match_info["cutout"].capitalize()
            file_format = request.match_info["file_format"]
            interval = request.query.get("interval")
            stretch = request.query.get("stretch")
            cmap = request.query.get("cmap", None)

            known_cutouts = ["Science", "Template", "Difference"]
            if cutout not in known_cutouts:
                return web.json_response(
                    {
                        "status": "error",
                        "message": f"cutout {cutout} not in {str(known_cutouts)}",
                    },
                    status=400,
                )
            known_file_formats = ["fits", "png"]
            if file_format not in known_file_formats:
                return web.json_response(
                    {
                        "status": "error",
                        "message": f"file format {file_format} not in {str(known_file_formats)}",
                    },
                    status=400,
                )

            normalization_methods = {
                "asymmetric_percentile": AsymmetricPercentileInterval(
                    lower_percentile=1, upper_percentile=100
                ),
                "min_max": MinMaxInterval(),
                "zscale": ZScaleInterval(nsamples=600, contrast=0.045, krej=2.5),
            }
            if interval is None:
                interval = "asymmetric_percentile"
            normalizer = normalization_methods.get(
                interval.lower(),
                AsymmetricPercentileInterval(lower_percentile=1, upper_percentile=100),
            )

            stretching_methods = {
                "linear": LinearStretch,
                "log": LogStretch,
                "asinh": AsinhStretch,
                "sqrt": SqrtStretch,
            }
            if stretch is None:
                stretch = "log" if cutout != "Difference" else "linear"
            stretcher = stretching_methods.get(stretch.lower(), LogStretch)()

            if (cmap is None) or (
                cmap.lower() not in ["bone", "gray", "cividis", "viridis", "magma"]
            ):
                cmap = "bone"
            else:
                cmap = cmap.lower()

            alert = await request.app["mongo"]["ZTF_alerts"].find_one(
                {"candid": candid}, {f"cutout{cutout}": 1}, max_time_ms=10000
            )

            cutout_data = loads(dumps([alert[f"cutout{cutout}"]["stampData"]]))[0]

            # unzipped fits name
            fits_name = pathlib.Path(alert[f"cutout{cutout}"]["fileName"]).with_suffix(
                ""
            )

            # unzip and flip about y axis on the server side
            with gzip.open(io.BytesIO(cutout_data), "rb") as f:
                with fits.open(io.BytesIO(f.read()), ignore_missing_simple=True) as hdu:
                    header = hdu[0].header
                    data_flipped_y = np.flipud(hdu[0].data)

            if file_format == "fits":
                hdu = fits.PrimaryHDU(data_flipped_y, header=header)
                # hdu = fits.PrimaryHDU(data_flipped_y)
                hdul = fits.HDUList([hdu])

                stamp_fits = io.BytesIO()
                hdul.writeto(fileobj=stamp_fits)

                return web.Response(
                    body=stamp_fits.getvalue(),
                    content_type="image/fits",
                    headers=MultiDict(
                        {"Content-Disposition": f"Attachment;filename={fits_name}"}
                    ),
                )

            if file_format == "png":
                buff = io.BytesIO()
                plt.close("all")
                fig = plt.figure()
                fig.set_size_inches(4, 4, forward=False)
                ax = plt.Axes(fig, [0.0, 0.0, 1.0, 1.0])
                ax.set_axis_off()
                fig.add_axes(ax)

                # replace nans with median:
                img = np.array(data_flipped_y)
                # replace dubiously large values
                xl = np.greater(np.abs(img), 1e20, where=~np.isnan(img))
                if img[xl].any():
                    img[xl] = np.nan
                if np.isnan(img).any():
                    median = float(np.nanmean(img.flatten()))
                    img = np.nan_to_num(img, nan=median)

                norm = ImageNormalize(img, stretch=stretcher)
                img_norm = norm(img)
                vmin, vmax = normalizer.get_limits(img_norm)
                ax.imshow(img_norm, cmap=cmap, origin="lower", vmin=vmin, vmax=vmax)

                plt.savefig(buff, dpi=42)

                buff.seek(0)
                plt.close("all")
                return web.Response(body=buff, content_type="image/png")

        except Exception as _e:
            log(f"Got error: {str(_e)}")
            _err = traceback.format_exc()
            log(_err)
            return web.json_response(
                {"status": "error", "message": f"failure: {_err}"}, status=400
            )
