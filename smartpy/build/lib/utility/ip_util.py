import geocoder


def getCity(ip):
    g = geocoder.ip(ip)
    return g.geojson['features'][0]['properties']['city']