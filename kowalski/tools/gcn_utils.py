import os
import traceback
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
from astropy.coordinates import ICRS, SkyCoord
from astropy.table import Table
from astropy.time import Time
from astropy_healpix import HEALPix, nside_to_level, pixel_resolution_to_nside
from shapely import get_parts, multipolygons
from shapely.geometry import MultiLineString
from shapely.ops import polygonize


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


def get_skymap_metadata(root, notice_type):
    """Get the skymap for a GCN notice."""

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


def get_contour(skymap):
    # Calculate credible levels.
    prob = flat_2d(skymap)
    cls = 100 * ligo.skymap.postprocess.find_greedy_credible_levels(prob)

    # Construct contours and return as a GeoJSON feature collection.
    levels = [50, 90]
    paths = ligo.skymap.postprocess.contour(cls, levels, degrees=True, simplify=True)
    center = ligo.skymap.postprocess.posterior_max(prob)
    contours = [
        {
            "type": "Point",
            "coordinates": [center.ra.deg - 180, center.dec.deg],
            "properties": {"credible_level": 0},
        }
    ] + [
        {
            "type": "MultiLineString",
            "properties": {"credible_level": level},
            # path is gonna be a list of lists of coordinates (lon, lat)
            # we remove 180 from the longitude to make it work with mongo
            # 'coordinates': [[[e[0]-180, e[1]] for e in line] for line in path]
            "coordinates": path,
        }
        for level, path in zip(levels, paths)
    ]

    # for each geometries that is a MultiLineString, we need to convert to a Polygon instead
    # for i, geometry in enumerate(contours):
    #     if geometry["type"] != 'MultiLineString':
    #         continue

    # polygon = Polygon(geometry['coordinates'][0])
    # for line in geometry['coordinates'][1:]:
    #     polygon = polygon.difference(MultiLineString(line).buffer(0))
    # #contour['geometries'][i]['coordinates'] = [[[e[0]-180, e[1]] for e in line] for line in polygon.exterior.coords]
    # coords = list(polygon.exterior.coords)
    # coords = [[e[0]-180, e[1]] for e in coords]
    # contours[i]['coordinates'] = [coords]
    # # save the coordinates to disk
    # with open('polygon_coords.txt', 'w') as f:
    #     f.write(str(contours[i]['coordinates']))
    # contours[i]['type'] = 'Polygon'

    for i, geometry in enumerate(contours):
        if geometry["type"] != "MultiLineString":
            continue
        try:
            multilinestring = MultiLineString(geometry["coordinates"])
            polygons = multipolygons(get_parts(polygonize(multilinestring)))
            contours[i]["coordinates"] = [
                [[[e[0] - 180, e[1]] for e in line]]
                for line in [
                    list(polygon.exterior.coords) for polygon in polygons.geoms
                ]
            ]
            contours[i]["type"] = "MultiPolygon"
        except Exception as e:
            print(e)
            print(traceback.format_exc())

    return contours
