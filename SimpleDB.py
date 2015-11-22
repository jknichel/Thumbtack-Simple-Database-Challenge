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
        if len(self.__transactions) == 0:
            self.__db[var] = val
        else:
            self.__transactions[-1][var] = val

    def get(self, var):
        """Get value of var in DB"""
        if len(self.__transactions) == 0:
            return self.__db.get(var, False)

        working_db = self.build_working_db()

        if var in working_db:
            return working_db[var]
        else:
            return False

    def unset(self, var):
        """If value exists, set it to None for this transaction"""
        if len(self.__transactions) == 0:
            self.__db.pop(var, None)
        else:
            self.__transactions[-1][var] = None

    def num_equal_to(self, val):
        """Get number of vars equal to val"""
        working_db = self.build_working_db()
        num_equal_to = 0

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
            return False

        del self.__transactions[-1]

        return True

    def commit(self):
        """Commits changes from all transactions to self.__db"""
        if len(self.__transactions) == 0:
            return False

        self.__db = self.build_working_db()
        self.__transactions = []

        return True

    def build_working_db(self):
        """Builds a version of what db would look like after a commit"""
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

def process_command(command, db):
    command = command.split(' ')
    if command[0] == 'END':
        return

    elif command[0] == 'SET':
        db.set(command[1], command[2])

    elif command[0] == 'GET':
        got = db.get(command[1])
        if got:
            print got
        else:
            print 'NULL'

    elif command[0] == 'UNSET':
        db.unset(command[1])

    elif command[0] == 'NUMEQUALTO':
        print db.num_equal_to(command[1])

    elif command[0] == 'BEGIN':
        db.begin()

    elif command[0] == 'ROLLBACK':
        anything_to_rollback = db.rollback()

        if not anything_to_rollback:
            print 'Nothing to ROLLBACK'

    elif command[0] == 'COMMIT':
        anything_to_commit = db.commit()

        if not anything_to_commit:
            print 'Nothing to COMMIT'

    else:
        print 'Not a valid command, please try again'


def main():
    db = SimpleDB()
    command = ''
    while command != 'END':
        command = raw_input('')
        process_command(command, db)

main()
