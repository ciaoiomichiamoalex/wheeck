import decimal
import json
import re
import time
from dataclasses import asdict, astuple, dataclass, field
from datetime import datetime
from difflib import SequenceMatcher
from enum import Enum

import pypdfium2

from common import Querier, get_logger
from config_share import *
from geo import GeoMap

__version__ = '1.0.0'

logger = get_logger(PATH_LOG, __name__)

@dataclass
class Delivery:
    document_number: int = field(init=False, default=None)
    document_genre: str = field(init=False, default=None)
    document_date: date = field(init=False, default=None)
    company_name: str = field(init=False, default=None)
    delivery_city: str = field(init=False, default=None)
    quantity: int = field(init=False, default=None)
    delivery_date: date = field(init=False, default=None)
    vehicle: str = field(init=False, default=None)
    vehicle_driver: str = field(init=False, default=None)
    distance: float = field(init=False, default=None)
    document_source: str
    page_number: int
    recording_date: datetime
    id_warning_message: int = field(init=False, default=None)

    def __str__(self) -> str:
        attributes = asdict(self)
        for key, value in attributes.items():
            if isinstance(value, datetime):
                attributes[key] = value.strftime('%Y-%m-%d %H:%M:%S')
            elif isinstance(value, date):
                attributes[key] = value.strftime('%Y-%m-%d')
            elif isinstance(value, decimal.Decimal):
                attributes[key] = float(value)
        return json.dumps(attributes)

    def calculate_distance(self) -> None:
        """
        Calculates distance between the attribute delivery city and the starting position in wheeck.json.
        """
        if self.delivery_city:
            with open(PATH_CFG_PRJ) as jin:
                departure = tuple(json.load(jin)['departure_coords'])

            geo: GeoMap = GeoMap()
            destination = geo.search(self.delivery_city)
            self.distance = geo.get_distance_from_coords(departure, destination) if destination else None

    def charge(self, attributes: dict) -> None:
        """
        Compile class attributes from the values of a dictionary.

        :param attributes: The attribute values.
        :type attributes: dict
        """
        for key, value in attributes.items():
            if hasattr(self, key):
                setattr(self, key, value)

class MessageGenre(Enum):
    DISCARD = 'Page %(page)d of doc %(doc)s discarded for error on %(pattern)s [number: %(document_number)d, genre: %(document_genre)s, date: %(document_date)s]'
    GAP = 'Found gap for doc number %(document_number)d of year %(document_year)d'
    WARNING = 'Had similarity crash for %(genre)s %(record)s on page %(page)d of doc %(doc)s'


def doc_scanner(working_doc: str, job_begin: datetime = datetime.now()) -> tuple[int, int]:
    """
    Scan a PDF document to extract its delivery information.

    :param working_doc: The path to the document.
    :type working_doc: str
    :param job_begin: The timestamp of the job starting.
    :type job_begin: datetime
    :return: The number of worked pages and discarded pages.
    :rtype: tuple[int, int]
    """
    querier: Querier = Querier(save_changes=True)
    # working_doc: path to document with suffix '.recording' (es. c:/source/wheeck/DDTs/2024_01_DDT_0001_0100.recording.pdf)
    # working_doc_name: basename of working_doc without suffix
    working_doc_name = working_doc.replace('.recording', '').split('/')[-1]

    # doc: raw PDF doc
    doc = pypdfium2.PdfDocument(working_doc)
    # doc_pages: total number of pages in working_doc
    doc_pages = len(doc)
    # discarded_pages: all information on pages with errors [anonymous class]
    discarded_pages = type('DiscardedPages', (object,), {
        # number: total number of pages with errors
        'number': 0,
        # on_error: pattern searching in error
        'on_error': None,
        # restart: increment number of one and set on_error to None
        'restart': lambda self: (setattr(self, 'number', self.number + 1), setattr(self, 'on_error', None))
    })()

    # working_page: current number page
    # page: raw PDF page
    for working_page, page in enumerate(doc, start=1):
        logger.info(f'scanning on page {working_page} of {working_doc_name}...')
        text = page.get_textpage().get_text_bounded()

        # delivery: information obtained from the page
        delivery: Delivery = Delivery(working_doc_name, working_page, job_begin)

        # getting document number, document genre and document date
        logger.info(f'searching for PATTERN_DOC_NUMBER on page {working_page} of {working_doc_name}...')
        search = re.search(PATTERN_DOC_NUMBER, text)
        if search:
            delivery.document_number = int(search.group(1).replace('.', ''))
            delivery.document_genre = search.group(2).upper()
            delivery.document_date = datetime.strptime(search.group(3), '%d/%m/%Y').date()

            logger.info(f'searching ok on page {working_page} of {working_doc_name}! [{delivery.document_number}, {delivery.document_genre}, {delivery.document_date}]')
        else:
            logger.error(f'discarding page {working_page} of {working_doc_name} for error on PATTERN_DOC_NUMBER...')
            discarded_pages.on_error = 'PATTERN_DOC_NUMBER' if not discarded_pages.on_error else f'{discarded_pages.on_error}, PATTERN_DOC_NUMBER'

        # getting delivery city
        logger.info(f'searching for PATTERN_CITY_DX on page {working_page} of {working_doc_name}...')
        search = re.search(PATTERN_CITY_DX, text)
        if search:
            delivery.company_name = search.group(1).upper().strip()
            delivery.delivery_city = search.group(2).upper().strip()

            logger.info(f'searching ok on page {working_page} of {working_doc_name}! [{delivery.company_name}, {delivery.delivery_city}]')
        else:
            logger.warning(f'error on PATTERN_CITY_DX on page {working_page} of {working_doc_name}... moving on PATTERN_CITY_SX!')
            # search on left side if there isn't on the right side of the document
            search = re.search(PATTERN_CITY_SX, text)
            if search:
                delivery.company_name = search.group(1).upper().strip()
                delivery.delivery_city = search.group(2).upper().strip()

                logger.info(f'searching ok on page {working_page} of {working_doc_name}! [{delivery.company_name}, {delivery.delivery_city}]')
            else:
                logger.error(f'discarding page {working_page} of {working_doc_name} for error on PATTERN_CITY...')
                discarded_pages.on_error = 'PATTERN_CITY' if not discarded_pages.on_error else f'{discarded_pages.on_error}, PATTERN_CITY'

        # getting quantity
        logger.info(f'searching for PATTERN_QUANTITY on page {working_page} of {working_doc_name}...')
        search = re.search(PATTERN_QUANTITY, text)
        if search:
            delivery.quantity = int(search.group(1).replace('.', ''))

            logger.info(f'searching ok on page {working_page} of {working_doc_name}! [{delivery.quantity}]')
        else:
            logger.error(f'discarding page {working_page} of {working_doc_name} for error on PATTERN_QUANTITY...')
            discarded_pages.on_error = 'PATTERN_QUANTITY' if not discarded_pages.on_error else f'{discarded_pages.on_error}, PATTERN_QUANTITY'

        # duplicate document date in delivery date
        logger.info(f'copying delivery date from document date on page {working_page} of {working_doc_name}...')
        delivery.delivery_date = delivery.document_date

        # getting vehicle and driver
        logger.info(f'searching for PATTERN_VEHICLE on page {working_page} of {working_doc_name}...')
        search = re.search(PATTERN_VEHICLE, text)
        if search:
            vehicle = search.group(1).upper()
            driver = search.group(2).upper() if search.group(2) else None

            with open(PATH_CFG_PRJ) as jin:
                members = json.load(jin)['members']
            vehicles_enum = [e['vehicle'] for e in members if e['vehicle']]
            drivers_enum = [e['driver'] for e in members if e['driver']]

            # check vehicle and driver with the possible value in wheeck.json
            delivery.vehicle = vehicle if vehicle in vehicles_enum else check_similarity(vehicle, vehicles_enum)
            if delivery.vehicle not in vehicles_enum:
                logger.warning(f'saving delivery warning for similarity crash on vehicle {delivery.vehicle} for page {working_page} of {working_doc_name}...')
                save_warning(MessageGenre.WARNING,
                             genre='vehicle',
                             record=delivery.vehicle,
                             page=delivery.page_number,
                             doc=delivery.document_source)

            if driver in drivers_enum:
                delivery.vehicle_driver = driver
            else:
                if driver == vehicle or driver in vehicles_enum:
                    driver = None

                calc_driver = check_similarity(driver, drivers_enum) if driver else None
                if driver and driver != calc_driver:
                    delivery.vehicle_driver = calc_driver
                else: delivery.vehicle_driver = next((e['driver'] for e in members if e['vehicle'] == delivery.vehicle), None)

            if delivery.vehicle_driver not in drivers_enum:
                logger.warning(f'saving delivery warning for similarity crash on driver {delivery.vehicle_driver} for page {working_page} of {working_doc_name}...')
                save_warning(MessageGenre.WARNING,
                             genre='driver',
                             record=delivery.vehicle_driver,
                             page=delivery.page_number,
                             doc=delivery.document_source)

            logger.info(f'searching ok on page {working_page} of {working_doc_name}! [{delivery.vehicle}, {delivery.vehicle_driver}]')
        else:
            logger.error(f'discarding page {working_page} of {working_doc_name} for error on PATTERN_VEHICLE...')
            discarded_pages.on_error = 'PATTERN_VEHICLE' if not discarded_pages.on_error else f'{discarded_pages.on_error}, PATTERN_VEHICLE'

        # check if is already a discard document
        search = re.search(PATTERN_DISCARD_DOC, working_doc_name)
        if search:
            # discard_doc_source: basename of the discard document (es. 2024_01_DDT_0001_0100.pdf)
            discard_doc_source = f'{search.group(1)}.pdf'
            # discard_page_number: the number after the first '_P' in the discard document name
            discard_page_number = int(search.group(2))
            logger.info(f'working doc {working_doc_name} is already a discard doc, source is {discard_doc_source} on page {discard_page_number}...')

            is_discard = querier.run(QUERY_GET_DISCARD, (discard_doc_source, discard_page_number))
            # check if there is already a discard record on database
            if is_discard and discarded_pages.on_error:
                discard = querier.fetch(Querier.FETCH_ONE)
                logger.info(f'found discard record of {working_doc_name}... [message id: {discard.id_warning_message}]')

                if None not in discard:
                    delivery.charge(dict(zip(querier.row_header(), discard)))
                    discarded_pages.on_error = None
                else:
                    logger.error(f'still found NULL cell on saving delivery from delivery discard in doc {working_doc_name}... skipping record!')
                    delivery.id_warning_message = discard.id_warning_message

        logger.info(f'searching for duplicate on page {working_page} of {working_doc_name}...')
        if not discarded_pages.on_error and check_duplicate(delivery):
            logger.error(f'discarding page {working_page} of {working_doc_name} for error on CHECK_DUPLICATE...')
            discarded_pages.on_error = 'CHECK_DUPLICATE'
            # don't save discard record on database in case of duplication
            delivery.id_warning_message = -1
        elif not delivery.distance:
            # check if distance for this city is already calculated on database
            logger.info(f'checking if the distance for {delivery.delivery_city} has already been calculated...')
            if querier.run(QUERY_GET_DISTANCE, (delivery.delivery_city,)) == 1:
                delivery.distance = querier.fetch(Querier.FETCH_VAL)
            else:
                logger.info(f'calculating distance from {delivery.delivery_city} for page {working_page} of {working_doc_name}...')
                delivery.calculate_distance()
                # waiting before next open route service API call (40 call/min)
                time.sleep(1.5)

        # save delivery record
        if not discarded_pages.on_error:
            logger.info(f'saving delivery {delivery} from page {working_page} of {working_doc_name}...')
            if querier.run(QUERY_INSERT_DELIVERY, astuple(delivery)[:-1]) != 1:
                logger.critical(f'error on saving delivery record from page {working_page} of {working_doc_name}... check the database connection!')
                logger.error(f'discarding page {working_page} of {working_doc_name} for error on INSERT_DELIVERY...')
                discarded_pages.on_error = 'INSERT_DELIVERY'
            elif delivery.id_warning_message and querier.run(QUERY_UPDATE_WARNING, (delivery.id_warning_message,)) != 1:
                logger.critical(f'error on updating delivery warning status from page {working_page} of {working_doc_name}... check the database connection! [message id: {delivery.id_warning_message}]')

        # check if there is errors on page
        if discarded_pages.on_error:
            if not delivery.id_warning_message:
                logger.info(f'saving discard record and warning message for page {working_page} of {working_doc_name} for errors on {discarded_pages.on_error}...')
                delivery.id_warning_message = save_warning(MessageGenre.DISCARD,
                                                           page=delivery.page_number,
                                                           doc=delivery.document_source,
                                                           pattern=discarded_pages.on_error,
                                                           document_number=delivery.document_number,
                                                           document_genre=delivery.document_genre,
                                                           document_date=delivery.document_date)
                if delivery.id_warning_message != -1 and querier.run(QUERY_INSERT_DISCARD, astuple(delivery)) != 1:
                    logger.critical(f'error on saving delivery discard record... check the database connection! [message id: {delivery.id_warning_message}]')

            discarded_doc = discard_doc(working_doc, working_page)
            logger.info(f'saving discard doc of page {working_page} on {working_doc_name}... [{discarded_doc.split('/')[-1]}]')
            discarded_pages.restart()

        # check if there is a warning gap to update status
        if querier.run(QUERY_GET_GAP, (
            delivery.document_number,
            delivery.document_date.year
        )):
            id_warning_gap = querier.fetch(Querier.FETCH_VAL)
            logger.info(f'checking if page {working_page} of {working_doc_name} is gap... ok! [message id: {id_warning_gap}]')
            if querier.run(QUERY_UPDATE_WARNING, (id_warning_gap,)) != 1:
                logger.critical(f'error on updating warning gap status from page {working_page} of {working_doc_name}... check the database connection! [message id: {id_warning_gap}]')

    doc.close()
    del querier
    return doc_pages, discarded_pages.number


def discard_doc(working_doc: str, working_page: int) -> str:
    """
    Generate a new PDF document by extracting a single page from another.

    :param working_doc: Path to the source document.
    :type working_doc: str
    :param working_page: Page number which must be extracted.
    :type working_page: int
    :return: The path to the new document.
    :rtype: str
    """
    # working_doc: path to document with suffix '.recording' (es. c:/source/wheeck/DDTs/2024_01_DDT_0001_0100.recording.pdf)
    # working_page: current number page
    # doc: raw PDF doc
    doc = pypdfium2.PdfDocument(working_doc)
    # discard: new raw PDF doc
    discard = pypdfium2.PdfDocument.new()

    discard.import_pages(doc, [working_page - 1])
    # discarded_doc: path to the new document (es. c:/source/wheeck/DDTs/discarded/2024_01_DDT_0001_0100_P001.pdf)
    discarded_doc = f"{PATH_DISCARDED_DIR}/{working_doc.replace('.recording.pdf', '').split('/')[-1]}_P{working_page:0>3}.pdf"
    discard.save(discarded_doc)

    discard.close()
    doc.close()
    return discarded_doc


def check_duplicate(delivery: Delivery) -> bool:
    """
    Check the existence of a delivery to avoid duplicates.

    :param delivery: The delivery object to check existence.
    :type delivery: Delivery
    :return: The existence or not.
    :rtype: bool
    """
    querier: Querier = Querier()
    querier.run(QUERY_CHECK_DUPLICATE, (
        delivery.document_source,
        delivery.page_number,
        delivery.document_number,
        delivery.document_genre,
        delivery.document_date.year
    ))
    is_duplicate = True if querier.fetch(Querier.FETCH_VAL) else False

    del querier
    return is_duplicate


def check_gaps() -> int:
    """
    Check the gaps in document number column and save warning messages.

    :return: The number of gaps found.
    :rtype: int
    """
    querier: Querier = Querier()
    gaps_num = querier.run(QUERY_CHECK_GAPS)

    for row in querier:
        save_warning(MessageGenre.GAP,
                     document_number=row.document_number,
                     document_year=row.document_year)
    del querier
    return gaps_num


def check_similarity(source_record: str, source_enum: list[str]) -> str:
    """
    Check the source_record similarity with records in source_enum list, in order to get the correct record.

    :param source_record: The starting record.
    :type source_record: str
    :param source_enum: The templates enum list.
    :type source_enum: list[str]
    :return: The record most similar or the starting record if the similarity index is lower than 50%.
    :rtype: str
    """
    logger.info(f'checking similarity for {source_record} in {source_enum}...')
    enum = dict.fromkeys(source_enum, 0.0)
    for record in enum:
        enum[record] = SequenceMatcher(None, record, source_record).ratio()

    max_score = sorted(enum, key=enum.get, reverse=True)[0]
    logger.info(f'found a similarity index of {round(enum[max_score] * 100)}% on {source_record}')
    return max_score if enum[max_score] > 0.5 else source_record


def save_warning(message_genre: MessageGenre, **kwargs) -> int:
    """
    Save a delivery warning message into the database and return the message ID.

    :param message_genre: The enum attribute that identifies the message genre.
    :type message_genre: MessageGenre
    :param kwargs: The named fields in the message text.
    :return: The ID of the message record on the database or -1 in case of error.
    :rtype: int
    """
    querier: Querier = Querier(save_changes=True)

    if querier.run(QUERY_INSERT_WARNING, (message_genre.name, message_genre.value % kwargs)) != 1:
        logger.critical('error on saving delivery warning message record... check the database connection!')
        id_message = -1
    else: id_message = querier.fetch(Querier.FETCH_VAL)

    del querier
    return id_message
