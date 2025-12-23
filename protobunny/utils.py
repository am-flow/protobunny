import datetime
import json
import uuid
from decimal import Decimal
from typing import Any

numpy_installed = False
try:
    import numpy as np

    numpy_installed = True
except ImportError:
    pass


class ProtobunnyJsonEncoder(json.JSONEncoder):
    def default(self, obj: object) -> Any:
        if isinstance(obj, datetime.datetime):
            if obj.tzinfo is None:
                obj = obj.replace(tzinfo=datetime.timezone.utc)
            return obj.isoformat()
        elif isinstance(obj, datetime.date):
            return obj.isoformat()
        elif isinstance(obj, uuid.UUID):
            return str(obj)
        elif isinstance(obj, bytes):
            return str(obj)
        elif isinstance(obj, Decimal):
            return float(obj)

        if not numpy_installed:
            return super().default(obj)
        else:
            if isinstance(obj, np.ndarray):
                return obj.tolist()
            elif isinstance(obj, np.integer):
                return int(obj)
            elif isinstance(obj, np.floating):
                return float(obj)
        return super().default(obj)
