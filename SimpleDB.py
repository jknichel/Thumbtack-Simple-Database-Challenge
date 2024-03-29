"""
Justin Knichel
Thumbtack Simple Database Challenge
"""

class SimpleDB(object):
    """Simple in-memory database"""
    def __init__(self):
        """Initialze instance of SimpleDB"""
        self.__db = {}              #permanent db entries reside here
        self.__transactions = []    #list of dictionaries for each transaction

    def set(self, var, val):
        """Set key var to value val in current transaction"""
        # if no open transactions, write directly to db
        if len(self.__transactions) == 0:
            self.__db[var] = val
        # if open transaction(s) write to transaction log
        else:
            self.__transactions[-1][var] = val

    def get(self, var):
        """Get value of var in DB"""
        # if no transactions, check db for var
        if len(self.__transactions) == 0:
            return self.__db.get(var, False)

        # generate current state of working database to scan for var
        working_db = self.build_working_db()

        # if value is in working_db, return it
        if var in working_db:
            return working_db[var]
        # otherwise return False to print NULL
        else:
            return False

    def unset(self, var):
        """If value exists, set it to None for this transaction"""
        # if no open transactions, pop directly from db
        if len(self.__transactions) == 0:
            self.__db.pop(var, None)
        # otherwise, set value to None in current transaction to indicate
        # it should be deleted on next commit
        else:
            self.__transactions[-1][var] = None

    def num_equal_to(self, val):
        """Get number of vars equal to val"""
        working_db = self.build_working_db()
        num_equal_to = 0

        # scan working database for the values, and count them
        for var in working_db:
            if working_db[var] == val:
                num_equal_to += 1

        return num_equal_to

    def begin(self):
        """Starts a new transaction"""
        self.__transactions.append({})

    def rollback(self):
        """Removes and ignores changes from most recent transaction"""
        if len(self.__transactions) == 0:
            # if no open transaction, raise exception defined below to indicate
            raise NoTransaction('NO TRANSACTION')

        del self.__transactions[-1]

        return True

    def commit(self):
        """Commits changes from all transactions to self.__db"""
        if len(self.__transactions) == 0:
            # if no open transaction, raise exception defined below to indicate
            raise NoTransaction('NO TRANSACTION')

        self.__db = self.build_working_db()
        self.__transactions = []

        return True

    def build_working_db(self):
        """Builds a version of what db would look like after a commit by
        collapsing all open transactions, keeping the most recent values for
        every variable, and writing, deleting, or overwriting to the original
        db as needed."""
        if len(self.__transactions) == 0:
            return self.__db

        working_db = {}

        reversed_transactions = self.__transactions[:]
        reversed_transactions.reverse()
        reversed_transactions.append(self.__db)

        for transaction in reversed_transactions:
            for var in transaction:
                if var not in working_db:
                    working_db[var] = transaction[var]

        return working_db

class NoTransaction(Exception):
    """An exception for attempt to close a transaction when there isn't one."""
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)


def process_command(command, db):
    """Take in command string, and act accordingly"""
    command = command.split(' ')
    if command[0] == 'END':
        return

    elif command[0] == 'SET':
        try:
            db.set(command[1], command[2])
        except:
            print 'Command format is \"SET variable value\". Please try again.'
            return

    elif command[0] == 'GET':
        try:
            got = db.get(command[1])
        except:
            print 'Command format is \"GET variable\". Please try again.'
            return

        if got:
            print got
        else:
            print 'NULL'

    elif command[0] == 'UNSET':
        try:
            db.unset(command[1])
        except:
            print 'Command format is \"UNSET variable\". Please try again.'
            return

    elif command[0] == 'NUMEQUALTO':
        try:
            print db.num_equal_to(command[1])
        except:
            print 'Command format is \"NUMEQUALTO value\". Please try again.'
            return

    elif command[0] == 'BEGIN':
        db.begin()

    elif command[0] == 'ROLLBACK':
        try:
            anything_to_rollback = db.rollback()
        except NoTransaction as message:
            print message.value

    elif command[0] == 'COMMIT':
        try:
            anything_to_commit = db.commit()
        except NoTransaction as message:
            print message.value

    else:
        print 'Not a valid command, please try again'

def main():
    """A main function to start the script and call process_command"""
    db = SimpleDB()
    command = ''
    while command != 'END':
        command = raw_input('')
        process_command(command, db)

if __name__ == '__main__':
    main()
