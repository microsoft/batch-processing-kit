class MockDevice:
    """
    A mock device to temporarily suppress output to device (stdout or stderr)
    Similar to UNIX /dev/null.
    """
    def write(self, s):
        pass
