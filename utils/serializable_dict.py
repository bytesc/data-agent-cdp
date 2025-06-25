import json
import mimetypes
from datetime import datetime, date
import pandas as pd


def serializable_dict(df):
    def json_serial(obj):
        if isinstance(obj, (datetime, pd.Timestamp)):
            return obj.isoformat()
        elif isinstance(obj, date):
            return obj.isoformat()
        return obj

    ans = json.loads(json.dumps(df, default=json_serial))
    return ans
