import decimal
import json
import os
import re
import shutil
import subprocess
import time
from dataclasses import asdict, astuple, dataclass, field
from datetime import date, datetime
from difflib import SequenceMatcher
from enum import Enum
from pathlib import Path

import pypdfium2

from core import Querier, decode_json, get_logger
from geo import GeoMap
from .constants import (CMD_BACKUP_DB, PATH_CFG, PATH_CFG_PRJ, PATH_DISCARDED_DIR, PATH_LOG, PATH_RECORDED_DIR,
                        PATH_SCHEME, PATH_WORKING_DIR, PATTERN_CITY_DX, PATTERN_CITY_SX, PATTERN_DISCARD_DOC,
                        PATTERN_DOC_NUMBER, PATTERN_QUANTITY, PATTERN_VEHICLE, PATTERN_WORKING_DOC,
                        QUERY_CHECK_DUPLICATE, QUERY_CHECK_GAPS, QUERY_GET_DISCARD, QUERY_GET_DISTANCE, QUERY_GET_GAP,
                        QUERY_INSERT_DELIVERY, QUERY_INSERT_DISCARD, QUERY_INSERT_WARNING, QUERY_UPDATE_WARNING)
from .overview_docs import generate_current

logger = get_logger(PATH_LOG, __name__)


@dataclass
class Delivery:
    """
    The Delivery object represents all the information extracted from a single PDF DDT page.
    """
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
            departure = tuple(decode_json(PATH_CFG_PRJ)['departure_coords'])
            geo: GeoMap = GeoMap(PATH_CFG_PRJ)
            destination = geo.search(self.delivery_city)
            self.distance = (geo.get_distance_from_coords(departure, destination)
                             if destination else None)

    def charge(self,
               attributes: dict) -> None:
        """
        Compile class attributes from the values of a dictionary.

        :param attributes: The attribute values.
        :type attributes: dict
        """
        for key, value in attributes.items():
            if hasattr(self, key): setattr(self, key, value)


class MessageGenre(Enum):
    DISCARD = 'Page %(page)d of doc %(doc)s discarded for error on %(pattern)s [number: %(document_number)d, genre: %(document_genre)s, date: %(document_date)s]'
    GAP = 'Found gap for doc number %(document_number)d of year %(document_year)d'
    WARNING = 'Had similarity crash for %(genre)s %(record)s on page %(page)d of doc %(doc)s'


def doc_scanner(working_doc: str | Path,
                job_begin: datetime = datetime.now()) -> tuple[int, int]:
    """
    Scan a DDT PDF document to extract delivery information.

    :param working_doc: The path to the document.
    :type working_doc: str | Path
    :param job_begin: The timestamp of the job starting.
    :type job_begin: datetime
    :return: The number of worked pages and discarded pages.
    :rtype: tuple[int, int]
    """
    querier: Querier = Querier(cfg_in=PATH_CFG, save_changes=True)
    # working_doc: path to document with suffix '.recording' (es. c:/source/wheeck/DDTs/2024_01_DDT_0001_0100.recording.pdf)
    # working_doc_name: basename of working_doc without suffix
    working_doc_name = working_doc.replace('.recording', '').rsplit('/', maxsplit=1)[-1]

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
        'restart': lambda self: (setattr(self, 'number', self.number + 1),
                                 setattr(self, 'on_error', None))
    })()

    # working_page: current number page
    # page: raw PDF page
    for working_page, page in enumerate(doc, start=1):
        logger.info('scanning on page %d of %s...', working_page, working_doc_name)
        text = page.get_textpage().get_text_bounded()

        # delivery: information obtained from the page
        delivery: Delivery = Delivery(working_doc_name, working_page, job_begin)

        # getting document number, document genre and document date
        logger.info('searching for PATTERN_DOC_NUMBER on page %d of %s...', working_page, working_doc_name)
        search = re.search(PATTERN_DOC_NUMBER, text)
        if search:
            delivery.document_number = int(search.group(1).replace('.', ''))
            delivery.document_genre = search.group(2).upper()
            delivery.document_date = datetime.strptime(search.group(3), '%d/%m/%Y').date()

            logger.info('searching ok on page %d of %s! [%d, %s, %s]', working_page, working_doc_name,
                        delivery.document_number, delivery.document_genre, delivery.document_date)
        else:
            logger.error('discarding page %d of %s for error on PATTERN_DOC_NUMBER...',
                         working_page, working_doc_name)
            discarded_pages.on_error = ('PATTERN_DOC_NUMBER' if not discarded_pages.on_error
                                        else f'{discarded_pages.on_error}, PATTERN_DOC_NUMBER')

        # getting delivery city
        logger.info('searching for PATTERN_CITY_DX on page %d of %s...', working_page, working_doc_name)
        search = re.search(PATTERN_CITY_DX, text)
        if search:
            delivery.company_name = search.group(1).upper().strip()
            delivery.delivery_city = search.group(2).upper().strip()

            logger.info('searching ok on page %d of %s! [%s, %s]',
                        working_page, working_doc_name, delivery.company_name, delivery.delivery_city)
        else:
            logger.warning('error on PATTERN_CITY_DX on page %d of %s... moving on PATTERN_CITY_SX!',
                           working_page, working_doc_name)
            # search on left side if there isn't on the right side of the document
            search = re.search(PATTERN_CITY_SX, text)
            if search:
                delivery.company_name = search.group(1).upper().strip()
                delivery.delivery_city = search.group(2).upper().strip()

                logger.info('searching ok on page %d of %s! [%s, %s]',
                            working_page, working_doc_name, delivery.company_name, delivery.delivery_city)
            else:
                logger.error('discarding page %d of %s for error on PATTERN_CITY...', working_page, working_doc_name)
                discarded_pages.on_error = ('PATTERN_CITY' if not discarded_pages.on_error
                                            else f'{discarded_pages.on_error}, PATTERN_CITY')

        # getting quantity
        logger.info('searching for PATTERN_QUANTITY on page %d of %s...', working_page, working_doc_name)
        search = re.search(PATTERN_QUANTITY, text)
        if search:
            delivery.quantity = int(search.group(1).replace('.', ''))

            logger.info('searching ok on page %d of %s! [%d]', working_page, working_doc_name, delivery.quantity)
        else:
            logger.error('discarding page %d of %s for error on PATTERN_QUANTITY...', working_page, working_doc_name)
            discarded_pages.on_error = ('PATTERN_QUANTITY' if not discarded_pages.on_error
                                        else f'{discarded_pages.on_error}, PATTERN_QUANTITY')

        # duplicate document date in delivery date
        logger.info('copying delivery date from document date on page %d of %s...', working_page, working_doc_name)
        delivery.delivery_date = delivery.document_date

        # getting vehicle and driver
        logger.info('searching for PATTERN_VEHICLE on page %d of %s...', working_page, working_doc_name)
        search = re.search(PATTERN_VEHICLE, text)
        if search:
            vehicle = search.group(1).upper()
            driver = search.group(2).upper() if search.group(2) else None

            members = decode_json(PATH_CFG_PRJ)['members']
            vehicles_enum = [e['vehicle'] for e in members if e['vehicle']]
            drivers_enum = [e['driver'] for e in members if e['driver']]

            # check vehicle and driver with the possible value in wheeck.json
            delivery.vehicle = (vehicle if vehicle in vehicles_enum
                                else check_similarity(vehicle, vehicles_enum))
            if delivery.vehicle not in vehicles_enum:
                logger.warning('saving delivery warning for similarity crash on vehicle %s for page %d of %s...',
                               delivery.vehicle, working_page, working_doc_name)
                save_warning(MessageGenre.WARNING,
                             genre='vehicle',
                             record=delivery.vehicle,
                             page=delivery.page_number,
                             doc=delivery.document_source)

            if driver in drivers_enum: delivery.vehicle_driver = driver
            else:
                if driver == vehicle or driver in vehicles_enum: driver = None

                calc_driver = check_similarity(driver, drivers_enum) if driver else None
                if driver and driver != calc_driver: delivery.vehicle_driver = calc_driver
                else: delivery.vehicle_driver = next((e['driver'] for e in members
                                                      if e['vehicle'] == delivery.vehicle), None)

            if delivery.vehicle_driver not in drivers_enum:
                logger.warning('saving delivery warning for similarity crash on driver %s for page %d of %s...',
                               delivery.vehicle_driver, working_page, working_doc_name)
                save_warning(MessageGenre.WARNING,
                             genre='driver',
                             record=delivery.vehicle_driver,
                             page=delivery.page_number,
                             doc=delivery.document_source)

            logger.info('searching ok on page %d of %s! [%s, %s]',
                        working_page, working_doc_name, delivery.vehicle, delivery.vehicle_driver)
        else:
            logger.error('discarding page %d of %s for error on PATTERN_VEHICLE...', working_page, working_doc_name)
            discarded_pages.on_error = ('PATTERN_VEHICLE' if not discarded_pages.on_error
                                        else f'{discarded_pages.on_error}, PATTERN_VEHICLE')

        # check if is already a discard document
        search = re.search(PATTERN_DISCARD_DOC, working_doc_name)
        if search:
            # discard_doc_source: basename of the discard document (es. 2024_01_DDT_0001_0100.pdf)
            discard_doc_source = f'{search.group(1)}.pdf'
            # discard_page_number: the number after the first '_P' in the discard document name
            discard_page_number = int(search.group(2))
            logger.info('working doc %s is already a discard doc, source is %s on page %d...',
                        working_doc_name, discard_doc_source, discard_page_number)

            is_discard = querier.run(QUERY_GET_DISCARD, discard_doc_source, discard_page_number).rows
            # check if there is already a discard record on database
            if is_discard and discarded_pages.on_error:
                discard = querier.fetch(Querier.FETCH_ONE)
                logger.info('found discard record of %s... [message id: %d]', working_doc_name, discard.id_warning_message)

                if None not in discard:
                    delivery.charge(dict(zip(querier.row_header(), discard)))
                    discarded_pages.on_error = None
                else:
                    logger.error('still found NULL cell on saving delivery from delivery discard in doc %s... skipping record!', working_doc_name)
                    delivery.id_warning_message = discard.id_warning_message

        logger.info('searching for duplicate on page %d of %s...', working_page, working_doc_name)
        if not discarded_pages.on_error and check_duplicate(delivery):
            logger.error('discarding page %d of %s for error on CHECK_DUPLICATE...', working_page, working_doc_name)
            discarded_pages.on_error = 'CHECK_DUPLICATE'
            # don't save discard record on database in case of duplication
            delivery.id_warning_message = -1
        elif not delivery.distance:
            # check if distance for this city is already calculated on database
            logger.info('checking if the distance for %s has already been calculated...', delivery.delivery_city)
            if querier.run(QUERY_GET_DISTANCE, delivery.delivery_city).rows == 1:
                delivery.distance = querier.fetch(Querier.FETCH_VAL)
            else:
                logger.info('calculating distance from %s for page %d of %s...',
                            delivery.delivery_city, working_page, working_doc_name)
                delivery.calculate_distance()
                # waiting before next open route service API call (40 call/min)
                time.sleep(1.5)

        # save delivery record
        if not discarded_pages.on_error:
            logger.info('saving delivery %s from page %d of %s...', delivery, working_page, working_doc_name)
            if querier.run(QUERY_INSERT_DELIVERY, *astuple(delivery)[:-1]).rows != 1:
                logger.critical('error on saving delivery record from page %d of %s... check the database connection!',
                                working_page, working_doc_name)
                logger.error('discarding page %d of %s for error on INSERT_DELIVERY...',
                             working_page, working_doc_name)
                discarded_pages.on_error = 'INSERT_DELIVERY'
            elif delivery.id_warning_message and querier.run(QUERY_UPDATE_WARNING, delivery.id_warning_message).rows != 1:
                logger.critical('error on updating delivery warning status from page %d of %s... check the database connection! [message id: %d]',
                                working_page, working_doc_name, delivery.id_warning_message)

        # check if there is errors on page
        if discarded_pages.on_error:
            if not delivery.id_warning_message:
                logger.info('saving discard record and warning message for page %d of %s for errors on %s...',
                            working_page, working_doc_name, discarded_pages.on_error)
                delivery.id_warning_message = save_warning(MessageGenre.DISCARD,
                                                           page=delivery.page_number,
                                                           doc=delivery.document_source,
                                                           pattern=discarded_pages.on_error,
                                                           document_number=delivery.document_number,
                                                           document_genre=delivery.document_genre,
                                                           document_date=delivery.document_date)
                if delivery.id_warning_message != -1 and querier.run(QUERY_INSERT_DISCARD, *astuple(delivery)).rows != 1:
                    logger.critical('error on saving delivery discard record... check the database connection! [message id: %d]', delivery.id_warning_message)

            discarded_doc = discard_doc(working_doc, working_page)
            logger.info('saving discard doc of page %d on %s... [%s]',
                        working_page, working_doc_name, discarded_doc.rsplit('/', maxsplit=1)[-1])
            discarded_pages.restart()

        # check if there is a warning gap to update status
        if querier.run(QUERY_GET_GAP, delivery.document_number, delivery.document_date.year).rows:
            id_warning_gap = querier.fetch(Querier.FETCH_VAL)
            logger.info('checking if page %d of %s is gap... ok! [message id: %d]',
                        working_page, working_doc_name, id_warning_gap)
            if querier.run(QUERY_UPDATE_WARNING, id_warning_gap).rows != 1:
                logger.critical('error on updating warning gap status from page %d of %s... check the database connection! [message id: %d]',
                                working_page, working_doc_name, id_warning_gap)

    doc.close()
    del querier
    return doc_pages, discarded_pages.number


def discard_doc(working_doc: str | Path,
                working_page: int) -> str:
    """
    Generate a new PDF document by extracting a single page from another.

    :param working_doc: Path to the source document.
    :type working_doc: str | Path
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
    discarded_doc = PATH_DISCARDED_DIR / f"{working_doc.replace('.recording.pdf', '').rsplit('/', maxsplit=1)[-1]}_P{working_page:0>3}.pdf"
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
    querier: Querier = Querier(cfg_in=PATH_CFG)
    is_duplicate = bool(querier.run(QUERY_CHECK_DUPLICATE,
        delivery.document_source,
        delivery.page_number,
        delivery.document_number,
        delivery.document_genre,
        delivery.document_date.year
    ).fetch(Querier.FETCH_VAL))

    del querier
    return is_duplicate


def check_gaps() -> int:
    """
    Check the gaps in document number column and save warning messages.

    :return: The number of gaps found.
    :rtype: int
    """
    querier: Querier = Querier(cfg_in=PATH_CFG)
    gaps_num = querier.run(QUERY_CHECK_GAPS).rows

    for row in querier:
        save_warning(MessageGenre.GAP,
                     document_number=row.document_number,
                     document_year=row.document_year)
    del querier
    return gaps_num


def check_similarity(source_record: str,
                     source_enum: list[str]) -> str:
    """
    Check the source_record similarity with records in source_enum list, in order to get the correct record.

    :param source_record: The starting record.
    :type source_record: str
    :param source_enum: The templates enum list.
    :type source_enum: list[str]
    :return: The record most similar or the starting record if the similarity index is lower than 50%.
    :rtype: str
    """
    logger.info('checking similarity for %s in %s...', source_record, source_enum)
    enum = dict.fromkeys(source_enum, 0.0)
    for record in enum:
        enum[record] = SequenceMatcher(None, record, source_record).ratio()

    max_score = sorted(enum, key=enum.get, reverse=True)[0]
    logger.info('found a similarity index of %s%% on %s', round(enum[max_score] * 100), source_record)
    return max_score if enum[max_score] > 0.5 else source_record


def save_warning(message_genre: MessageGenre,
                 **kwargs) -> int:
    """
    Save a delivery warning message into the database and return the message ID.

    :param message_genre: The enum attribute that identifies the message genre.
    :type message_genre: MessageGenre
    :param kwargs: The named fields in the message text.
    :return: The ID of the message record on the database or -1 in case of error.
    :rtype: int
    """
    querier: Querier = Querier(cfg_in=PATH_CFG, save_changes=True)

    if querier.run(QUERY_INSERT_WARNING, message_genre.name, message_genre.value % kwargs).rows != 1:
        logger.critical('error on saving delivery warning message record... check the database connection!')
        id_message = -1
    else: id_message = querier.fetch(Querier.FETCH_VAL)

    del querier
    return id_message


def run() -> None:
    """
    Run the scanner_doc function for every document in the WORKING_DIR.
    """
    # docs: list of documents in PATH_WORKING_DIR (es. [2024_01_DDT_0001_0100.pdf, ...])
    docs = sorted(next(os.walk(PATH_WORKING_DIR), (None, None, []))[2])
    logger.info('DDTs dir content %s', docs)
    job_begin = None

    # doc: single document from the list (es. 2024_01_DDT_0001_0100.pdf)
    for doc in docs:
        if not re.search(PATTERN_WORKING_DOC, doc):
            logger.warning('error on PATTERN_WORKING_DOC for %s... skipping doc!', doc)
            continue

        job_begin = datetime.now()
        logger.info('JOB START: working on doc %s...', doc)
        # working_doc: path to document with suffix '.recording' (es. c:/source/wheeck/DDTs/2024_01_DDT_0001_0100.recording.pdf)
        working_doc = PATH_WORKING_DIR / f"{doc.split('.')[0]}.recording.pdf"
        shutil.move(PATH_WORKING_DIR / doc, working_doc)

        # worked_pages: total number of pages in working_doc
        # discarded_pages: total number of pages with error in working_doc
        worked_pages, discarded_pages = doc_scanner(working_doc, job_begin)

        logger.info('JOB END: worked %d pages in %s [%d discarded pages]', worked_pages, doc, discarded_pages)
        # make backup copy of worked document if defined in the configuration
        if not re.search(PATTERN_DISCARD_DOC, working_doc.replace('.recording', '').rsplit('/', maxsplit=1)[-1]):
            backup_dir = decode_json(PATH_CFG_PRJ).get('backup_dir')
            if backup_dir:
                logger.info('copying worked doc %s in backup dir...', doc)
                shutil.copy(working_doc, f'{backup_dir}/{doc}')
        shutil.move(working_doc, PATH_RECORDED_DIR / f"{doc.split('.')[0]}.recorded.pdf")

    if job_begin:
        # gaps: total number of document number gaps found
        gaps = check_gaps()
        logger.info('checking gaps... %d gaps found!', gaps)
        # update overview and summary of recorded documents
        logger.info('generate or update overview and summary of recorded docs...')
        generate_current()

        # make backup copy of database schema
        try:
            subprocess.run(CMD_BACKUP_DB, shell=True, check=True, stderr=subprocess.PIPE, text=True)

            # keep only last valid database backup copy
            backup_copy, origin_copy = PATH_SCHEME / f'wheeck.bak.dump', PATH_SCHEME / f'wheeck.dump'
            backup_size = os.path.getsize(backup_copy)
            try: origin_size = os.path.getsize(origin_copy)
            except FileNotFoundError: origin_size = 0

            if backup_size >= origin_size: shutil.move(backup_copy, origin_copy)
            else: os.remove(backup_copy)
        except subprocess.CalledProcessError as error:
            logger.critical('error during backup database schema... [%s]', error.stderr)
