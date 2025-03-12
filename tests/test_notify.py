from myairflow.send_notify import send_noti

def test_notify():
    msg = "pytest:cho"
    a = send_noti(msg)
    assert True
    assert a==204
