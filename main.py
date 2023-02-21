#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


import sys

from destination_dhis2 import DestinationDhis2

if __name__ == "__main__":
    DestinationDhis2().run(sys.argv[1:])
