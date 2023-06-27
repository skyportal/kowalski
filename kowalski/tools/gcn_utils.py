import base64
import json
import os
import urllib
from tempfile import NamedTemporaryFile
from urllib.parse import urlparse

import astropy.units as u
import gcn
import healpy as hp
import ligo.skymap.bayestar as ligo_bayestar
import ligo.skymap.distance
import ligo.skymap.io
import ligo.skymap.moc
import ligo.skymap.postprocess
import numpy as np
import requests
import scipy
from astropy.coordinates import ICRS, Angle, Latitude, Longitude, SkyCoord
from astropy.table import Table
from astropy.time import Time
from astropy_healpix import HEALPix, nside_to_level, pixel_resolution_to_nside
from mocpy import MOC


def get_trigger(root):
    """Get the trigger ID from a GCN notice."""

    property_name = "TrigID"
    path = f".//Param[@name='{property_name}']"
    elem = root.find(path)
    if elem is None:
        return None
    value = elem.attrib.get("value", None)
    if value is not None:
        value = int(value)

    return value


def get_dateobs(root):
    """Get the UTC event time from a GCN notice, rounded to the nearest second,
    as a datetime.datetime object."""
    dateobs = Time(
        root.find(
            "./WhereWhen/{*}ObsDataLocation"
            "/{*}ObservationLocation"
            "/{*}AstroCoords"
            "[@coord_system_id='UTC-FK5-GEO']"
            "/Time/TimeInstant/ISOTime"
        ).text,
        precision=0,
    )

    # FIXME: https://github.com/astropy/astropy/issues/7179
    dateobs = Time(dateobs.iso)

    return dateobs.datetime


def get_aliases(root):
    aliases = []
    notice_type = gcn.get_notice_type(root)
    try:
        # we try to find aliases in the notice itself, which the user can update on the frontend by fetching data from TACH
        if notice_type in [
            gcn.NoticeType.FERMI_GBM_FIN_POS,
            gcn.NoticeType.FERMI_GBM_FLT_POS,
            gcn.NoticeType.FERMI_GBM_GND_POS,
        ]:
            url = root.find("./What/Param[@name='LightCurve_URL']").attrib["value"]
            alias = url.split("/triggers/")[1].split("/")[1].split("/")[0]
            aliases.append(f"FERMI#{alias}")

        # we try the LVC convention
        graceid = root.find("./What/Param[@name='GraceID']")
        if graceid is not None:
            aliases.append(f"LVC#{graceid.attrib['value']}")
    except Exception as e:
        print(f"Could not find aliases in notice: {str(e)}")
        pass

    return aliases


def get_skymap_url(root, notice_type):
    url = None
    available = False
    # Try Fermi GBM convention
    if notice_type == gcn.NoticeType.FERMI_GBM_FIN_POS:
        url = root.find("./What/Param[@name='LocationMap_URL']").attrib["value"]
        url = url.replace("http://", "https://")
        url = url.replace("_locplot_", "_healpix_")
        url = url.replace(".png", ".fit")

    # Try Fermi GBM **subthreshold** convention. Stupid, stupid, stupid!!
    if notice_type == gcn.NoticeType.FERMI_GBM_SUBTHRESH:
        url = root.find("./What/Param[@name='HealPix_URL']").attrib["value"]

    skymap = root.find("./What/Group[@type='GW_SKYMAP']")
    if skymap is not None and url is None:
        children = skymap.getchildren()
        for child in children:
            if child.attrib["name"] == "skymap_fits":
                url = child.attrib["value"]
                break

    if url is not None:
        # we have a URL, but is it available? We don't want to download the file here,
        # so we'll just check the HTTP status code.
        try:
            response = requests.head(url, timeout=5)
            if response.status_code == 200:
                available = True
        except requests.exceptions.RequestException:
            pass

    return url, available


def is_retraction(root):
    retraction = root.find("./What/Param[@name='Retraction']")
    if retraction is not None:
        retraction = int(retraction.attrib["value"])
        if retraction == 1:
            return True
    return False


def get_skymap_cone(root):
    ra, dec, error = None, None, None
    mission = urlparse(root.attrib["ivorn"]).path.lstrip("/")
    # Try error cone
    loc = root.find("./WhereWhen/ObsDataLocation/ObservationLocation")
    if loc is None:
        return ra, dec, error

    ra = loc.find("./AstroCoords/Position2D/Value2/C1")
    dec = loc.find("./AstroCoords/Position2D/Value2/C2")
    error = loc.find("./AstroCoords/Position2D/Error2Radius")

    if None in (ra, dec, error):
        return ra, dec, error

    ra, dec, error = float(ra.text), float(dec.text), float(error.text)

    # Apparently, all experiments *except* AMON report a 1-sigma error radius.
    # AMON reports a 90% radius, so for AMON, we have to convert.
    if mission == "AMON":
        error /= scipy.stats.chi(df=2).ppf(0.95)

    return ra, dec, error


def get_skymap_metadata(root):
    """Get the skymap for a GCN notice."""

    notice_type = gcn.get_notice_type(root)
    skymap_url, available = get_skymap_url(root, notice_type)
    if skymap_url is not None and available:
        return "available", {"url": skymap_url, "name": skymap_url.split("/")[-1]}
    elif skymap_url is not None and not available:
        # can we get a cone?
        ra, dec, error = get_skymap_cone(root)
        if None not in (ra, dec, error):
            return "cone", {
                "ra": ra,
                "dec": dec,
                "error": error,
                "name": f"{ra:.5f}_{dec:.5f}_{error:.5f}",
            }
        else:
            return "unavailable", {"url": skymap_url, "name": skymap_url.split("/")[-1]}

    if is_retraction(root):
        return "retraction", None

    ra, dec, error = get_skymap_cone(root)
    if None not in (ra, dec, error):
        return "cone", {
            "ra": ra,
            "dec": dec,
            "error": error,
            "name": f"{ra:.5f}_{dec:.5f}_{error:.5f}",
        }

    return "missing", None


def from_ellipse(localization_name, ra, dec, amaj, amin, phi):

    max_depth = 10
    NSIDE = int(2**max_depth)
    hpx = HEALPix(NSIDE, "nested", frame=ICRS())
    ipix = MOC.from_elliptical_cone(
        lon=Longitude(ra, u.deg),
        lat=Latitude(dec, u.deg),
        a=Angle(amaj, unit="deg"),
        b=Angle(amin, unit="deg"),
        pa=Angle(np.mod(phi, 180.0), unit="deg"),
        max_depth=max_depth,
    ).flatten()

    # Convert to multi-resolution pixel indices and sort.
    uniq = ligo.skymap.moc.nest2uniq(nside_to_level(NSIDE), ipix.astype(np.int64))
    i = np.argsort(uniq)
    ipix = ipix[i]
    uniq = uniq[i]

    probdensity = np.ones(ipix.shape)
    probdensity /= probdensity.sum() * hpx.pixel_area.to_value(u.steradian)

    skymap = {
        "localization_name": localization_name,
        "uniq": uniq.tolist(),
        "probdensity": probdensity.tolist(),
    }

    return skymap


def from_cone(ra, dec, error, n_sigma=4):
    localization_name = f"{ra:.5f}_{dec:.5f}_{error:.5f}"

    center = SkyCoord(ra * u.deg, dec * u.deg)
    radius = error * u.deg

    # Determine resolution such that there are at least
    # 16 pixels across the error radius.
    hpx = HEALPix(
        pixel_resolution_to_nside(radius / 16, round="up"), "nested", frame=ICRS()
    )

    # Find all pixels in the 4-sigma error circle.
    ipix = hpx.cone_search_skycoord(center, n_sigma * radius)

    # Convert to multi-resolution pixel indices and sort.
    uniq = ligo.skymap.moc.nest2uniq(nside_to_level(hpx.nside), ipix.astype(np.int64))
    i = np.argsort(uniq)
    ipix = ipix[i]
    uniq = uniq[i]

    # Evaluate Gaussian.
    distance = hpx.healpix_to_skycoord(ipix).separation(center)
    probdensity = np.exp(
        -0.5 * np.square(distance / radius).to_value(u.dimensionless_unscaled)
    )
    probdensity /= probdensity.sum() * hpx.pixel_area.to_value(u.steradian)

    skymap = {
        "localization_name": localization_name,
        "uniq": uniq.tolist(),
        "probdensity": probdensity.tolist(),
    }

    return skymap


def from_bytes(name, content):
    def get_col(m, name):
        try:
            col = m[name]
        except KeyError:
            return None
        else:
            return col.tolist()

    filename, filecontent = None, None
    # file file is a bytes object, so we need to convert it to a file-like object
    if isinstance(content, bytes) or isinstance(content, str):
        if isinstance(content, str):
            try:
                if "base64," in content:
                    arrSplit = content.split("base64,")[1]
                    filename = arrSplit[0].split("name=")[-1].replace(";", "")
                    filename = urllib.parse.unquote(filename)
                    filecontent = arrSplit[-1]
                else:
                    filename = name
                    filecontent = base64.b64decode(content.encode("utf-8"))
            except Exception:
                raise TypeError("Could not decode file content")
        else:
            filename = name
            filecontent = content
    else:
        raise TypeError("file must be a bytes object or a string")

    # open a temporary file to store the skymap
    nside = 128
    skymap = None
    occulted = None
    with NamedTemporaryFile() as f:
        f.write(filecontent)
        f.flush()
        skymap = ligo.skymap.io.read_sky_map(f.name, moc=True)
        if skymap is None:
            return None
        occulted = get_occulted(f.name, nside=nside)
    if occulted is not None:
        order = hp.nside2order(nside)
        skymap_flat = ligo_bayestar.rasterize(skymap, order)["PROB"]
        skymap_flat = hp.reorder(skymap_flat, "NESTED", "RING")
        skymap_flat[occulted] = 0.0
        skymap_flat = skymap_flat / skymap_flat.sum()
        skymap_flat = hp.reorder(skymap_flat, "RING", "NESTED")
        skymap = ligo_bayestar.derasterize(Table([skymap_flat], names=["PROB"]))

    skymap = {
        "localization_name": filename,
        "uniq": get_col(skymap, "UNIQ"),
        "probdensity": get_col(skymap, "PROBDENSITY"),
        "distmu": get_col(skymap, "DISTMU"),
        "distsigma": get_col(skymap, "DISTSIGMA"),
        "distnorm": get_col(skymap, "DISTNORM"),
    }

    return skymap


def from_url(url):
    def get_col(m, name):
        try:
            col = m[name]
        except KeyError:
            return None
        else:
            return col.tolist()

    filename = os.path.basename(urlparse(url).path)

    skymap = ligo.skymap.io.read_sky_map(url, moc=True)

    nside = 128
    occulted = get_occulted(url, nside=nside)
    if occulted is not None:
        order = hp.nside2order(nside)
        skymap_flat = ligo_bayestar.rasterize(skymap, order)["PROB"]
        skymap_flat = hp.reorder(skymap_flat, "NESTED", "RING")
        skymap_flat[occulted] = 0.0
        skymap_flat = skymap_flat / skymap_flat.sum()
        skymap_flat = hp.reorder(skymap_flat, "RING", "NESTED")
        skymap = ligo_bayestar.derasterize(Table([skymap_flat], names=["PROB"]))

    skymap = {
        "localization_name": filename,
        "uniq": get_col(skymap, "UNIQ"),
        "probdensity": get_col(skymap, "PROBDENSITY"),
        "distmu": get_col(skymap, "DISTMU"),
        "distsigma": get_col(skymap, "DISTSIGMA"),
        "distnorm": get_col(skymap, "DISTNORM"),
    }

    return skymap


def from_polygon(localization_name, polygon):

    center, corners = None, None
    if isinstance(polygon, str):
        try:
            polygon = json.loads(polygon)
        except Exception:
            raise TypeError("Could not parse polygon string")

    if isinstance(polygon, dict) and "center" in polygon and "corners" in polygon:
        center, corners = polygon["center"], polygon["corners"]
    elif isinstance(polygon, list):
        center, corners = None, polygon
    else:
        raise TypeError(
            "polygon must be a dict with keys 'center' and 'corners', or a list of length > 3 with each element being (ra, dec) tuple"
        )

    if center is not None:
        if (
            not (isinstance(center, list) or isinstance(center, tuple))
            or len(center) != 2
        ):
            raise TypeError("center must be a 2-tuple of floats or ints")
        for coord in center:
            if not isinstance(coord, float) and not isinstance(coord, int):
                raise TypeError("center must be a 2-tuple of floats or ints")

    if isinstance(corners, list) and len(corners) < 3:
        raise TypeError(
            "polygon must be a list of at least 3 2-tuples of floats or ints"
        )
    for point in corners:
        if not (isinstance(point, list) or isinstance(point, tuple)) or len(point) != 2:
            raise TypeError("polygon must be a list of 2-tuples of floats or ints")
        for coord in point:
            if not isinstance(coord, float) and not isinstance(coord, int):
                raise TypeError("polygon must be a list of 2-tuples of floats or ints")

    xyz = [hp.ang2vec(r, d, lonlat=True) for r, d in corners]
    ipix = None
    nside = 1024  # order 10
    while nside < 2**30:  # until order 29
        try:
            hpx = HEALPix(nside, "nested", frame=ICRS())
            ipix = hp.query_polygon(hpx.nside, np.array(xyz), nest=True)
        except Exception:
            nside *= 2
            continue
        if ipix is None or len(ipix) == 0:
            nside *= 2
        else:
            break

    if ipix is None or len(ipix) == 0:
        raise ValueError("No pixels found in polygon.")

    # Convert to multi-resolution pixel indices and sort.
    uniq = ligo.skymap.moc.nest2uniq(nside_to_level(hpx.nside), ipix.astype(np.int64))
    i = np.argsort(uniq)
    ipix = ipix[i]
    uniq = uniq[i]

    # Evaluate Gaussian.
    probdensity = np.ones(ipix.shape)
    probdensity /= probdensity.sum() * hpx.pixel_area.to_value(u.steradian)

    skymap = {
        "localization_name": localization_name,
        "uniq": uniq.tolist(),
        "probdensity": probdensity.tolist(),
        "center": center,
    }

    return skymap


def get_occulted(url, nside=64):

    m = Table.read(url, format="fits")
    ra = m.meta.get("GEO_RA", None)
    dec = m.meta.get("GEO_DEC", None)
    error = m.meta.get("GEO_RAD", 67.5)

    if (ra is None) or (dec is None) or (error is None):
        return None

    center = SkyCoord(ra * u.deg, dec * u.deg)
    radius = error * u.deg

    hpx = HEALPix(nside, "ring", frame=ICRS())

    # Find all pixels in the circle.
    ipix = hpx.cone_search_skycoord(center, radius)

    return ipix


@staticmethod
def table_2d(skymap):
    """Get multiresolution HEALPix dataset, probability density only."""
    return Table(
        [np.asarray(skymap["uniq"], dtype=np.int64), skymap["probdensity"]],
        names=["UNIQ", "PROBDENSITY"],
    )


@staticmethod
def flat_2d(skymap):
    """Get flat resolution HEALPix dataset, probability density only."""
    nside = 512
    order = hp.nside2order(nside)
    result = ligo_bayestar.rasterize(table_2d(skymap), order)["PROB"]
    return hp.reorder(result, "NESTED", "RING")


def get_contour(skymap, levels):
    # Calculate credible levels.
    prob = flat_2d(skymap)
    cls = 100 * ligo.skymap.postprocess.find_greedy_credible_levels(prob)

    # Construct contours and return as a GeoJSON feature collection.
    paths = ligo.skymap.postprocess.contour(cls, levels, degrees=True, simplify=True)
    center = ligo.skymap.postprocess.posterior_max(prob)
    center = (
        [center.ra.deg, center.dec.deg]
        if skymap.get("center") is None
        else [skymap.get("center")[0] - 180.0, skymap.get("center")[1]]
    )
    contours = [
        {
            "type": "Point",
            "coordinates": center,
            "properties": {"credible_level": 0},
        }
    ] + [
        {
            "type": "MultiPolygon",
            "properties": {"credible_level": level},
            # path is gonna be a list of lists of coordinates (lon, lat)
            # we remove 180 from the longitude to make it work with mongo
            "coordinates": [[[[e[0] - 180, e[1]] for e in line]] for line in path]
            # "coordinates": path,
        }
        for level, path in zip(levels, paths)
    ]

    return contours


def from_voevent(root):
    """
    Generate a skymap from a voevent

    :param alert:
    :return:
    """
    status, skymap_metadata = get_skymap_metadata(root)

    if status == "available":
        return from_url(skymap_metadata["url"])
    elif status == "cone":
        return from_cone(
            ra=skymap_metadata["ra"],
            dec=skymap_metadata["dec"],
            error=skymap_metadata["error"],
        )
    else:
        return None


def get_contours(skymap, contour_levels=[90, 95]):
    """
    Generate a contour from a skymap

    :param skymap:
    :return:
    """
    contour_levels_keys = ["contour{}".format(level) for level in contour_levels]
    contours = get_contour(skymap, levels=contour_levels)
    return {"center": contours[0], **dict(zip(contour_levels_keys, contours[1:]))}


def from_dict(skymap_data):
    dict_keys = set(["localization_name", "uniq", "probdensity"])
    url_keys = set(["url"])
    file_keys = set(["localization_name", "content"])
    cone_keys = set(["ra", "dec", "error"])
    polygon_keys = set(["localization_name", "polygon"])
    ellipse_keys = set(["localization_name", "ra", "dec", "amaj", "amin", "phi"])

    if dict_keys.issubset(set(skymap_data.keys())):
        skymap = skymap_data  # its already usable as is
    elif url_keys.issubset(set(skymap_data.keys())):
        skymap = from_url(skymap_data["url"])
    elif file_keys.issubset(set(skymap_data.keys())):
        skymap = from_bytes(skymap_data["localization_name"], skymap_data["content"])
    elif cone_keys.issubset(set(skymap_data.keys())):
        skymap = from_cone(skymap_data["ra"], skymap_data["dec"], skymap_data["error"])
    elif polygon_keys.issubset(set(skymap_data.keys())):
        skymap = from_polygon(skymap_data["localization_name"], skymap_data["polygon"])
    elif ellipse_keys.issubset(set(skymap_data.keys())):
        skymap = from_ellipse(
            skymap_data["localization_name"],
            skymap_data["ra"],
            skymap_data["dec"],
            skymap_data["amaj"],
            skymap_data["amin"],
            skymap_data["phi"],
        )
    else:
        raise Exception("Invalid skymap data")

    return skymap
