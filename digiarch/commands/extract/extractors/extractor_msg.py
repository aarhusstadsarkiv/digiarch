from contextlib import suppress
from pathlib import Path
from re import IGNORECASE
from re import match
from typing import ClassVar

import chardet
from acacore.models.file import BaseFile
from extract_msg import Attachment
from extract_msg import AttachmentBase
from extract_msg import Message
from extract_msg import MSGFile
from extract_msg import openMsg
from extract_msg import SignedAttachment
from extract_msg.exceptions import ExMsgBaseException
from extract_msg.msg_classes import MessageBase
from extract_msg.msg_classes import MessageSigned
from olefile import MINIMAL_OLEFILE_SIZE
from RTFDE.exceptions import MalformedEncapsulatedRtf

from digiarch.common import sanitize_filename
from digiarch.common import TempDir

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


def validate_msg(file: BaseFile) -> Message | MessageSigned:
    try:
        msg: MSGFile = openMsg(file.get_absolute_path(), delayAttachments=True)
        _ = msg.attachments
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

    with suppress(AttributeError, UnicodeDecodeError, MalformedEncapsulatedRtf):
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


def msg_attachment(attachment: AttachmentBase) -> MessageBase | None:
    if not attachment.data:
        return None

    if isinstance(attachment.data, MessageBase):
        return attachment.data

    if isinstance(attachment.data, bytes):
        # noinspection PyTypeChecker
        if len(attachment.data) < MINIMAL_OLEFILE_SIZE:
            return None

        try:
            msg = openMsg(attachment.data, delayAttachments=True)
        except (ExMsgBaseException, FileNotFoundError, ValueError):
            return None

        return msg if isinstance(msg, MessageBase) else None

    raise TypeError(f"Unsupported attachment data type {type(attachment.data)}")


def msg_attachments(
    msg: Message,
    body_html: str | None,
    body_rtf: str | None,
) -> tuple[list[AttachmentBase], list[AttachmentBase | SignedAttachment | Message | MessageSigned]]:
    inline_attachments: list[AttachmentBase | SignedAttachment] = []
    attachments: list[AttachmentBase | SignedAttachment | Message | MessageSigned] = []

    for attachment in msg.attachments:
        try:
            if attachment.data is None:
                continue
            if (cid := getattr(attachment, "cid", None)) and cid in (body_html or body_rtf or ""):
                inline_attachments.append(attachment)
            elif attachment_msg := msg_attachment(attachment):
                if isinstance(attachment_msg, (Message, MessageSigned)):
                    # noinspection PyTypeChecker
                    attachments.append(attachment_msg)
            else:
                name = attachment.longFilename if isinstance(attachment, SignedAttachment) else attachment.getFilename()
                if name and any(match(pattern, name, flags=IGNORECASE) for pattern in EXCLUDED_ATTACHMENTS):
                    continue
                attachments.append(attachment)
        except NotImplementedError:
            continue

    return inline_attachments, attachments


def prepare_attachment_name(names: list[str], name: str, n: int) -> [tuple[str], str, str]:
    """Deduplicate attachment name by attaching a prefix to the sanitized name with the index of that name if it has already been extracted."""
    name = name.strip() or f"attachment-{n}"
    name_sanitized: str = sanitize_filename(name, 20, True).strip("_") or f"attachment-{n}"
    names.append(name_sanitized.lower())
    if (count := names.count(name_sanitized.lower())) > 1:
        name_sanitized = f"{count - 1}_{name_sanitized}"
    return names, name, name_sanitized


class MsgExtractor(ExtractorBase):
    tool_names: ClassVar[list[str]] = ["msg"]

    def extract(self) -> list[tuple[Path, Path]]:
        extract_folder: Path = self.extract_folder
        files: list[tuple[str, str]] = []

        msg: Message | MessageSigned = validate_msg(self.file)
        _, body_html, body_rtf = msg_body(msg)
        inline_attachments, attachments = msg_attachments(msg, body_html, body_rtf)

        with TempDir(self.file.root) as tmp_dir:
            names: list[str] = []
            for n, attachment in enumerate(inline_attachments + attachments):
                if isinstance(attachment, (Message, MessageSigned)):
                    name: str = (attachment.filename or "").strip() or (attachment.subject or "").strip()
                    names, name, name_sanitized = prepare_attachment_name(names, name, n)
                    attachment.export(tmp_dir / name_sanitized)
                    files.append((name_sanitized, name))
                elif isinstance(attachment.data, bytes):
                    name: str = (
                        attachment.getFilename()
                        if isinstance(attachment, Attachment)
                        else attachment.longFilename or ""
                    )
                    names, name, name_sanitized = prepare_attachment_name(names, name, n)
                    with tmp_dir.joinpath(name_sanitized).open("wb") as fh:
                        fh.write(attachment.data or b"")
                    files.append((name_sanitized, name))
                elif attachment.data is not None:
                    raise ExtractError(self.file, f"Cannot extract attachment of type {type(attachment.data)}")

            if not files:
                return []

            extract_folder.mkdir(parents=True, exist_ok=True)

            return [
                (tmp_dir.joinpath(name).replace(extract_folder.joinpath(name)), extract_folder.joinpath(name_original))
                for name, name_original in files
            ]
