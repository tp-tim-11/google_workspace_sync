from typing import Protocol


class GoogleExecuteRequest(Protocol):
    def execute(self) -> dict[str, object]: ...


class DriveFilesApi(Protocol):
    def list(self, **kwargs: object) -> GoogleExecuteRequest: ...

    def export(self, **kwargs: object) -> object: ...

    def get_media(self, **kwargs: object) -> object: ...


class DriveService(Protocol):
    def files(self) -> DriveFilesApi: ...


class SheetsValuesApi(Protocol):
    def get(self, **kwargs: object) -> GoogleExecuteRequest: ...


class SheetsSpreadsheetsApi(Protocol):
    def values(self) -> SheetsValuesApi: ...


class SheetsService(Protocol):
    def spreadsheets(self) -> SheetsSpreadsheetsApi: ...
