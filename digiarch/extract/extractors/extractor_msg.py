from contextlib import suppress
from pathlib import Path
from re import match
from typing import ClassVar
from typing import Generator

import chardet
from acacore.models.file import File
from extract_msg import AttachmentBase
from extract_msg import Message
from extract_msg import MSGFile
from extract_msg import openMsg
from extract_msg import SignedAttachment
from extract_msg.exceptions import ExMsgBaseException
from extract_msg.msg_classes import MessageSigned

from digiarch.doctor import sanitize_path

from .base import ExtractError
from .base import ExtractorBase
from .base import NotPreservableFileError
from .base import UnrecognizedFileError

EXCLUDED_ATTACHMENTS: list[str] = [
    "tunnel_marking.txt",
    "tunnel marking.txt",
    "fesdPacket.xml",
    "signature-*.txt",
    "smime.p7m",
]


def validate_msg(file: File) -> Message | MessageSigned:
    try:
        msg: MSGFile = openMsg(file.get_absolute_path(), delayAttachments=True)
    except ExMsgBaseException as e:
        raise UnrecognizedFileError(file, e.args[0] if e.args else "File cannot be opened as msg")

    if not isinstance(msg, (Message, MessageSigned)):
        raise NotPreservableFileError(file, f"Is of type {msg.__class__.__name__}")

    return msg


# noinspection PyUnusedLocal
def msg_body(msg: Message) -> tuple[str | None, str | None, str | None]:
    body_txt: str | None = None
    body_html: str | None = None
    body_rtf: str | None = None

    with suppress(AttributeError, UnicodeDecodeError):
        body_txt = (msg.body or "").strip()

    with suppress(AttributeError, UnicodeDecodeError):
        body_html_bytes: bytes | None = msg.htmlBody
        if body_html_bytes is not None:
            encoding: str | None = chardet.detect(body_html_bytes).get("encoding") or "utf-8"
            body_html = body_html_bytes.decode(encoding)

    with suppress(AttributeError, UnicodeDecodeError):
        body_rtf_bytes: bytes | None = msg.rtfBody
        if body_rtf_bytes is not None:
            encoding: str | None = chardet.detect(body_rtf_bytes).get("encoding") or "utf-8"
            body_rtf = body_rtf_bytes.decode(encoding)

    return body_txt, body_html, body_rtf


def msg_attachment(attachment: AttachmentBase) -> Message | bool | None:
    try:
        if isinstance(attachment.data, (Message, MessageSigned)):
            attachment_msg = attachment.data
        elif not attachment.data:
            return None
        elif isinstance(attachment.data, (str, bytes)):
            attachment_msg = openMsg(attachment.data, delayAttachments=True)
        else:
            raise TypeError(f"Unsupported attachment data type {type(attachment.data)}")
    except (ExMsgBaseException, FileNotFoundError):
        return None

    return attachment_msg if isinstance(attachment_msg, (Message, MessageSigned)) else False


def msg_attachments(
    msg: Message,
    body_html: str | None,
    body_rtf: str | None,
) -> tuple[list[AttachmentBase], list[AttachmentBase | SignedAttachment | Message | MessageSigned]]:
    inline_attachments: list[AttachmentBase | SignedAttachment] = []
    attachments: list[AttachmentBase | SignedAttachment | Message | MessageSigned] = []

    for attachment in msg.attachments:
        if (cid := getattr(attachment, "cid", None)) and cid in (body_html or body_rtf or ""):
            inline_attachments.append(attachment)
        elif (attachment_msg := msg_attachment(attachment)) is False:
            continue
        elif attachment_msg is not None:
            attachments.append(attachment_msg)
        else:
            name = attachment.longFilename if isinstance(attachment, SignedAttachment) else attachment.getFilename()
            if any(match(pattern, name.lower()) for pattern in EXCLUDED_ATTACHMENTS):
                continue
            attachments.append(attachment)

    return inline_attachments, attachments


class MsgExtractor(ExtractorBase):
    tool_names: ClassVar[list[str]] = ["msg"]

    def extract(self) -> Generator[Path, None, None]:
        extract_folder: Path = self.extract_folder
        extract_folder.mkdir(parents=True, exist_ok=True)

        msg: Message | MessageSigned = validate_msg(self.file)
        _, body_html, body_rtf = msg_body(msg)
        inline_attachments, attachments = msg_attachments(msg, body_html, body_rtf)

        for attachment in inline_attachments + attachments:
            if isinstance(attachment, (Message, MessageSigned)):
                path: Path = extract_folder.joinpath(sanitize_path(attachment.filename))
                if path.suffix != ".msg":
                    path.with_name(path.name + ".msg")
                attachment.export(path)
                yield path
            elif attachment.data is not None and not isinstance(attachment.data, bytes):
                raise ExtractError(self.file, f"Cannot extract attachment with data of type {type(attachment.data)}")
            else:
                name = attachment.longFilename if isinstance(attachment, SignedAttachment) else attachment.getFilename()
                path: Path = extract_folder.joinpath(sanitize_path(name))
                with path.open("wb") as fh:
                    # noinspection PyTypeChecker
                    fh.write(attachment.data or b"")
                yield path

        yield from ()
