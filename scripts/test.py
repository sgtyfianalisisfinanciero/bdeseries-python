import bdeseries

print(bdeseries.get_data_path())

bdeseries.initialize(download_catalog=True)

print(bdeseries.get_data_path())
