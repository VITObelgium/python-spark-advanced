"""
Requests a time sequence of tiles, splits them up in sub-tiles (line-based) and reduces them (calculates their average)
to a single tile again.
"""

from datetime import datetime
from catalogclient.catalog import Catalog
from pyspark import SparkContext
import rasterio
import numpy as np
import sys

TILE_WIDTH = 3360
TILE_HEIGHT = 3360
ROWS_PER_TILE = 4


class NdviTile(object):
    def __init__(self, eoproduct):
        self.x = eoproduct.tilex
        self.y = eoproduct.tiley
        self.ndvi_file = NdviTile._extract_ndvi_file(eoproduct)

    @staticmethod
    def _extract_ndvi_file(eoproduct):
        prefix = "file:"

        ndvi_files = [file.filename[len(prefix):]
                      for file in eoproduct.files
                      if file.filename.startswith(prefix) and 'NDVI' in file.bands]

        if not ndvi_files:
            raise RuntimeError('no NDVI file found', eoproduct)
        else:
            return ndvi_files[0]


class NdviSubTile(object):
    def __init__(self, ndvi_tile, row, total_rows):
        self.x = ndvi_tile.x
        self.y = ndvi_tile.y
        self.row = row
        self.image_data = self._extract_image_data(ndvi_tile.ndvi_file, self.row, total_rows)

    @staticmethod
    def _extract_image_data(ndvi_file, row, total_rows):
        import numpy

        with rasterio.open(ndvi_file) as src:
            band = src.read()[0]
            (height, width) = band.shape

            if width != TILE_WIDTH or height != TILE_HEIGHT:
                raise RuntimeError('expected width/height %i/%i, got %i, %i instead' %
                                   (TILE_WIDTH, TILE_HEIGHT, width, height))

            return numpy.vsplit(band, total_rows)[row]


def main(argv):
    output_dir = argv[1] if len(argv) > 1 else '.'

    products = Catalog().get_products('PROBAV_L3_S1_TOC_333M', fileformat='GEOTIFF',
                                      startdate=datetime(2016, 1, 1), enddate=datetime(2016, 2, 28),
                                      min_lon=-2.5, max_lon=6.5, min_lat=49.5, max_lat=51.5)

    sc = SparkContext(appName='subtile_average')

    try:
        # the list of NDVI tiles for the entire bounding box, for all times
        ndvi_tiles = sc.parallelize(products).map(NdviTile)

        # split tiles into sub-tiles: expand [(x, y)] to [(x, y, row)]
        ndvi_sub_tiles = ndvi_tiles\
            .flatMap(lambda tile: [NdviSubTile(tile, row, ROWS_PER_TILE) for row in range(ROWS_PER_TILE)])

        # create pairs [((x, y, row), sub_image)]
        sub_image_at_position = ndvi_sub_tiles\
            .map(lambda sub_tile: ((sub_tile.x, sub_tile.y, sub_tile.row), sub_tile.image_data))

        # sum and count sub-images per position: [((x, y, row), [(total, count)])]
        sub_image_sums = sub_image_at_position.aggregateByKey(
            zeroValue=(np.zeros((TILE_HEIGHT / ROWS_PER_TILE, TILE_WIDTH)), 0),
            seqFunc=lambda (total, count), image_data: (total + image_data, count + 1),
            combFunc=lambda (total0, count0), (total1, count1): (total0 + total1, count0 + count1)
        )

        # calculate sub-image average from sum and count
        sub_image_averages = sub_image_sums.map(lambda (location, (total, count)): (location, total / count))

        # re-group sub-image averages by tile (x, y) again: [((x, y), [((x, y, row), average)]], then sort them by
        # row and only keep the averaged image data
        tile_averages = sub_image_averages\
            .groupBy(lambda ((x, y, _), __): (x, y))\
            .mapValues(lambda rows: sorted(rows, key=lambda ((_, __, row), average): row))\
            .mapValues(lambda rows: map(lambda ((_, __, ___), average): average, rows))

        def write_image(tile_average):
            (x, y), rows = tile_average

            output_file = '%s/subtile_average_%i_%i.tif' % (output_dir, x, y)
            joined_image_data = np.concatenate(tuple(rows))

            with rasterio.open(output_file, mode='w', driver='GTiff', width=TILE_WIDTH, height=TILE_HEIGHT, count=1,
                               dtype=rasterio.uint8) as dst:
                dst.write(joined_image_data.astype(rasterio.uint8), 1)

        tile_averages.foreach(write_image)
    finally:
        sc.stop()


if __name__ == '__main__':
    main(sys.argv)
