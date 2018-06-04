#
# downcast - tools for unpacking patient data from DWC
#
# Copyright (c) 2018 Laboratory for Computational Physiology
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

class Error(Exception):
    """Base exception type for database errors."""
    pass

class InterfaceError(Error):
    """Base exception type relating to the database interface."""
    pass

class DatabaseError(Error):
    """Base exception type relating to the database."""
    pass

class OperationalError(DatabaseError):
    """Exception caused by an error in database operation."""
    pass

class DataSyntaxError(OperationalError):
    """Exception caused by a malformed entry in the data file."""
    pass

class ProgrammingError(DatabaseError):
    """Exception caused by errors in the query syntax."""
    pass

class ParameterCountError(ProgrammingError):
    """Exception caused by supplying the wrong number of query parameters."""
    pass

class DataError(DatabaseError):
    """Exception caused by an error in processed data."""
    pass

class IntegrityError(DatabaseError):
    """Exception caused by an error in database integrity."""
    pass

class InternalError(DatabaseError):
    """Exception caused by an internal database error."""
    pass

class NotSupportedError(DatabaseError):
    """Exception caused by an unsupported operation."""
    pass
