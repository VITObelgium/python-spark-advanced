class NdviTile(object):
    TILE_WIDTH = 3360
    TILE_HEIGHT = TILE_WIDTH

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
        self.ndvi_file = ndvi_tile.ndvi_file
        self.row = row
        self.total_rows = total_rows

    def image_data(self):
        import rasterio
        import numpy

        with rasterio.open(self.ndvi_file) as src:
            band = src.read()[0]
            (height, width) = band.shape

            if width != NdviTile.TILE_WIDTH or height != NdviTile.TILE_HEIGHT:
                raise RuntimeError('expected width/height %i/%i, got %i, %i instead' %
                                   (NdviTile.TILE_WIDTH, NdviTile.TILE_HEIGHT, width, height))

            return numpy.vsplit(band, self.total_rows)[self.row]
