import esa_vito
file = 'c_gls_SWI1km_202203121200_CEURO_SCATSAR_V1.0.1.nc'
e = esa_vito.Vito()
e.work_path = "."

lat = 38.21189038321695
lon = -1.4834937740030583
info = e.extract_data(file, lat, lon)

print(info)
