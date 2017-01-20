"""
This Spark application will request a time sequence of tiles, split them up into sub-tiles, reduce those sub-tiles in
time (in this case, calculate their mean) in a distributed way, re-assemble those sub-tiles and write them to files.
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

    def mean(sub_tiles):
        """Returns the mean of the images within a collection of sub-tiles."""
        result = np.zeros((NdviTile.TILE_HEIGHT / ROWS_PER_TILE, NdviTile.TILE_WIDTH))

        for sub_tile in sub_tiles:
            result += sub_tile.image_data()

        return (result / len(sub_tiles)).astype(rasterio.uint8)

    try:
        # the list of NDVI tiles for the entire bounding box, for all times
        ndvi_tiles = sc.parallelize(products).map(NdviTile)

        # split tiles into sub-tiles: expand [(x, y)] to [(x, y, row)]
        ndvi_sub_tiles = ndvi_tiles\
            .flatMap(lambda tile: [NdviSubTile(tile, row, ROWS_PER_TILE) for row in range(ROWS_PER_TILE)])

        # create pairs [((x, y, row), sub_tile)]
        sub_tile_at_position = ndvi_sub_tiles\
            .map(lambda sub_tile: ((sub_tile.x, sub_tile.y, sub_tile.row), sub_tile))

        # group sub-tiles by position (same x, y and row): [((x, y, row), [sub_tile])]
        sub_tiles_at_position = sub_tile_at_position.groupByKey()

        # reduce sub-tiles: in this case, calculate mean
        sub_image_averages = sub_tiles_at_position.mapValues(mean)

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
                dst.write(joined_image_data, 1)

        tile_averages.foreach(write_image)
    finally:
        sc.stop()


if __name__ == '__main__':
    main(sys.argv)
