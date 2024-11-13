from typing import Type

from acacore.models.file import OriginalFile

from .extractors.base import ExtractorBase
from .extractors.extractor_msg import MsgExtractor
from .extractors.extractor_patool import PatoolExtractor
from .extractors.extractor_tnef import TNEFExtractor
from .extractors.extractor_webarchive import WebarchiveExtractor
from .extractors.extractor_zip import ZipExtractor


def find_extractor(file: OriginalFile) -> tuple[Type[ExtractorBase] | None, str | None]:
    """
    Match an extractor class to a file.

    Matches the value at ``File.action_data.extract.tool`` to the ``ExtractorBase.tool_names`` of the various
    converters. The first matching extractor is returned. If multiple extractors support the same tool, then priority is
    given to the extractor matched first.
    :param file: The ``acacore.models.file.File`` object to find the extractor for.
    :return: An ``ExtractorBase`` class or ``None`` if an extractor could not be found, and the name of the tool.
    """
    if not file.action_data.extract:
        return None, None

    for extractor in (ZipExtractor, TNEFExtractor, MsgExtractor, PatoolExtractor, WebarchiveExtractor):
        if file.action_data.extract.tool in extractor.tool_names:
            return extractor, file.action_data.extract.tool

    return None, file.action_data.extract.tool
