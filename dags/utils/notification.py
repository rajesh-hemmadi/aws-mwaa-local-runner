#This will be used to send notifcation to users when process is failed

def send_notification(type="info",message=''):
    print('Sending Notifcation')
    print('notification type is :' , type)
    print('notification type is :' , message)
    print('Notification Sent')

    """Notes to delete
    Need to look at having audit/control table
    Store required details and load into audit table
    re look at having detailed stats"""