# Json-data-into-postgres
This repository consist codes on extracting data from EventFinda API and loading it into Postgres table for further analytics

## Overview

This project is about how to extract Json format data and store it into Postgres table.
The data is extracted daily and will be updated with local_datetime
Once the json format is downloaded , it wil be transformed into columns of data and to be stored inside Postgres.
The data is grabbed every day.

## Installation
The script is based on on virtualenv and therefore to activate it, navigate to bin/, type source bin/activate.
This will bring up the virtualenv and you can execute the code from the virtualenv without needing to install anything

