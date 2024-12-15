import json

import openrouteservice
from openrouteservice import directions, exceptions, geocode

from common import get_logger
from config_share import PATH_CFG_PRJ, PATH_LOG

__version__ = '1.0.0'

logger = get_logger(PATH_LOG, __name__)

class GeoMap:
    def __init__(self) -> None:
        """
        Read from config/wheeck.json the api key and open a client for the service calls.
        """
        with open(PATH_CFG_PRJ) as jin:
            api_key = json.load(jin)['ors_api_key']

        self._client = openrouteservice.Client(key=api_key)

    def search(self, address: str, country: str = 'IT') -> tuple[float, float] | None:
        """
        Return the coordinates of specific address.

        :param address: The address to searching for.
        :type address: str
        :param country: The country of the address in iso-2 or iso-3, defaults to 'IT'.
        :type country: str
        :return: The latitude and longitude coordinates.
        :rtype: tuple[float, float] | None
        """
        try:
            response = geocode.pelias_search(self._client, address, country=country)
            return tuple(response['features'][0]['geometry']['coordinates']) if response['features'] else None
        except exceptions.ApiError as e:
            logger.error(f'API error on searching ({address}, {country})! [status code: {e.status}, error code: {e.message['error']['code']}, error message: {e.message['error']['message']}]')
        except exceptions.HTTPError as e:
            logger.error(f'HTTP error on searching ({address}, {country})! [status code: {e.status_code}]')
        except exceptions.Timeout:
            logger.error(f'timeout error on searching ({address}, {country})!')

    def get_distance(self, departure: str, destination: str, departure_country: str = 'IT', destination_country: str = 'IT') -> float | None:
        """
        Return the distance in kilometers between two addresses, by passing address names.

        :param departure: The departure address.
        :type departure: str
        :param destination: The destination address.
        :type destination: str
        :param departure_country: The country of the departure address in iso-2 or iso-3, defaults to 'IT'.
        :type departure_country: str
        :param destination_country: The country of the destination address in iso-2 or iso-3, defaults to 'IT'.
        :type destination_country: str
        :return: The distance in kilometers.
        :rtype: float | None
        """
        departure_coords = self.search(departure, country=departure_country)
        destination_coords = self.search(destination, country=destination_country)
        return self.get_distance_from_coords(departure_coords, destination_coords) if departure_coords and destination_coords else None

    def get_distance_from_coords(self, departure: tuple[float, float], destination: tuple[float, float]) -> float:
        """
        Return the distance in kilometers between two address, by passing address coordinates.

        :param departure: The departure address coordinates.
        :type departure: tuple[float, float]
        :param destination: The destination address coordinates.
        :type destination: tuple[float, float]
        :return: The distance in kilometers.
        :rtype: float
        """
        try:
            route = directions.directions(client=self._client, coordinates=(departure, destination), radiuses=(500,), profile='driving-car', format='geojson')
            return route['features'][0]['properties']['segments'][0]['distance'] / 1000
        except exceptions.ApiError as e:
            logger.error(f'API error on calculating distance ({departure}, {destination})! [status code: {e.status}, error code: {e.message['error']['code']}, error message: {e.message['error']['message']}]')
        except exceptions.HTTPError as e:
            logger.error(f'HTTP error on calculating distance ({departure}, {destination})! [status code: {e.status_code}]')
        except exceptions.Timeout:
            logger.error(f'timeout error on calculating distance ({departure}, {destination})!')
