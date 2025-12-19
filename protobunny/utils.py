import datetime
import json
import uuid
from decimal import Decimal
from typing import Any

import numpy as np


class ProtobunnyJsonEncoder(json.JSONEncoder):
    def default(self, obj: object) -> Any:
        if isinstance(obj, (datetime.datetime, datetime.date)):
            return obj.isoformat()
        elif isinstance(obj, uuid.UUID):
            return str(obj)
        elif isinstance(obj, bytes):
            return str(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        elif isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, Decimal):
            return float(obj)
        return super().default(obj)
