import json
import sys
import tempfile
from google.cloud import spanner
from google.cloud.spanner import Client
from google.cloud.spanner_v1.database import Database
from google.cloud.spanner_v1.instance import Instance
from google.cloud.spanner_v1.transaction import Transaction
from google.oauth2 import service_account
from io import BytesIO
from logging import Logger
from pathlib import Path
from pypomes_core import file_get_data, exc_format
from types import TracebackType

from .db_common import _DB_CONN_DATA, DbEngine, DbParam


class GoogleSpanner:
    """
    An encapsulation of the Google Cloud Spanner access lifecycle.

    *Spanner* uses *Identity and Access Management* (IAM), rather than simple username/password strings, and thus
    the encapsulation replaces traditional RDBMS URL/User patterns with *GCP Resource Hierarchies*, handling
    the *Google Cloud*'s hierarchical structure, namely, **Project/Instance/Database*. The *google-cloud-spanner*
    library manages a connection pool under the hood, and to encapsulate this logic, this class takes care of the
    *Client* object, while providing access to a *Database* object.
    """
    def __init__(self,
                 *,
                 instance_id: str,
                 database_id: str,
                 project_id: str = None,
                 credentials: BytesIO | Path | str | bytes = None,
                 errors: list[str] = None,
                 logger: Logger = None) -> None:
        r"""
        Encapsulate the Google Cloud Spanner lifecycle.

        The nature of access *credentials* depends on its data type:
            - type *BytesIO*: *credentials* is a byte stream
            - type *Path*: *credentials* is a path to a file holding the data
            - type *bytes*: *credentials* holds the data (used as is)
            - type *str*: *credentials* holds the data (used as utf8-encoded)

        A credentials file for Google Cloud Spanner is a standard *JSON* service account key file containing
        specific fields like *project_id*, *private_key_id*, *private_key*, *client_email*, and *client_id*.
        The *google.oauth2.service_account* package uses this file to authenticate with Google Cloud:
            {
                "type": "service_account",
                "project_id": "my-gcp-project-id",
                "private_key_id": "a1b2c3d4e5f6g7h8i9j0k1l2m3n4o5p6q7r8s9t0",
                "private_key": "-----BEGIN PRIVATE KEY-----\\nMIIEvgIBADANBhEFAASC+...\\n-----END PRIVATE KEY-----\\n",
                "client_email": "my-service-account-name@my-gcp-project-id.iam.gserviceaccount.com",
                "client_id": "12345678901234567890",
                "auth_uri": "accounts.google.com",
                "token_uri": "oauth2.googleapis.com",
                "auth_provider_x509_cert_url": "www.googleapis.com",
                "client_x509_cert_url": "www.googleapis.com"
            }

        The parameter *project_id* is required if *credentials* is not provided, otherwise it is ignored.

        :param instance_id: the instance identification
        :param database_id: the databae identification
        :param project_id: optional project identification
        :param credentials: optional access credentials
        :param errors: incidental error messages (might be a non-empty list)
        :param logger: optional logger
        """
        # initialize the instance variables
        self._client: Client | None = None
        self._instance: Instance | None = None
        self._database: Database | None = None
        self._credentials_path: Path | None = None

        self._project_id: str = project_id
        self._instance_id: str = instance_id
        self._database_id: str = database_id

        if instance_id and database_id and credentials:
            credentials_bytes: bytes = file_get_data(file_data=credentials)
            credentials_json: dict[str, str] = json.loads(s=credentials_bytes)
            self._project_id = credentials_json["project_id"]
            if isinstance(credentials, Path):
                self._credentials_path = credentials.resolve()
            else:
                with tempfile.NamedTemporaryFile(mode="wb",
                                                 delete=False) as tmp:
                    tmp.write(credentials_bytes)
                    self._credentials_path = Path(tmp.name)

        if self._project_id and self._instance_id and self._database_id:
            _DB_CONN_DATA[DbEngine.SPANNER] = {
                DbParam.ENGINE: self
            }
        else:
            msg = "Unable to assign required properties"
            if logger:
                logger.error(msg=msg)
            if isinstance(errors, list):
                errors.append(msg)

    def assert_access(self,
                      errors: list[str] = None,
                      logger: Logger = None) -> bool:
        """
        Determine whether the current configuration allows for access to the Google Cloud Spanner database engine.

        This function should be invoked once, at application's initialization time. This is necessary
        to make sure access is possible with the provided parameters.

        :param errors: incidental error messages (might be a non-empty list)
        :param logger: optional logger
        """
        # initialize the return variable
        result: bool = True

        if not self._client:
            try:
                if self._credentials_path:
                    # Use a specific service account JSON key
                    credentials = service_account.Credentials.from_service_account_file(
                        filename=str(self._credentials_path)
                    )
                    self._client = spanner.Client(project=self._project_id,
                                                  credentials=credentials)
                else:
                    # fallback to Application Default Credentials (ADC)
                    self._client = spanner.Client(project=self._project_id)

                self._instance = self._client.instance(instance_id=self._instance_id)
                self._database = self._instance.database(database_id=self._database_id)
            except Exception as e:
                result = False
                exc_err: str = exc_format(exc=e,
                                          exc_info=sys.exc_info())
                if logger:
                    logger.error(msg=exc_err)
                if isinstance(errors, list):
                    errors.append(exc_err)

        return result

    def get_transactor(self) -> Transaction:
        """
        Retrieve a transaction object for explicit, multi-step Read-Write Transaction.

        :return: a transaction object to reproduce the  traditional *BEGIN/COMMIT/ROLLBACK* pattern.
        """
        # noinspection PyUnresolvedReferences
        return self._database.read_write_transaction()

    def __enter__(self) -> Database:
        """
        Entry point for context management.
        """
        return self._database

    def __exit__(self,
                 exc_type: type[BaseException] | None,
                 exc_val: BaseException | None,
                 exc_tb: TracebackType | None) -> None:
        """
        Clean up resources.

        While Spanner sessions are handled internally by a pool, explicit cleanup logic can be added here if needed.
        """
