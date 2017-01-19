"""
Requests a time sequence of tiles, splits them up in sub-tiles (line-based) and reduces them (calculates their average)
to a single tile again.
"""

from tiles.ndvi import NdviTile, NdviSubTile
from datetime import datetime
from catalogclient.catalog import Catalog
from pyspark import SparkContext
import rasterio
import numpy as np
import sys

ROWS_PER_TILE = 4


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
            zeroValue=(np.zeros((NdviTile.TILE_HEIGHT / ROWS_PER_TILE, NdviTile.TILE_WIDTH)), 0),
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

            with rasterio.open(output_file, mode='w', driver='GTiff', width=NdviTile.TILE_WIDTH, height=NdviTile.TILE_HEIGHT, count=1,
                               dtype=rasterio.uint8) as dst:
                dst.write(joined_image_data.astype(rasterio.uint8), 1)

        tile_averages.foreach(write_image)
    finally:
        sc.stop()


if __name__ == '__main__':
    main(sys.argv)
