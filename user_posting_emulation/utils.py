from datetime import datetime

def json_serializer(obj: datetime) -> str:
    '''
    Serialize datetime to ISO format.
        
    Parameters:
    ----------
    obj: datetime
        Datetime object to convert to ISO format
            
    Returns:
    -------
    obj_iso: str
            Formatted datetime
    '''
    if isinstance(obj, datetime):
        obj_iso = obj.isoformat()
        return obj_iso
    raise TypeError("Type not serializable")
