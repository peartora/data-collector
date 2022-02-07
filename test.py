from sqlalchemy import true


cp_numbers = 10

def checkNumbers(cp_numbers):
        if(cp_numbers > 100):
            return True
        else:
            raise Exception("Wrong clinching-type")


try:
    checkNumbers(cp_numbers)
except:
    print('xxx')
else:
    print('yyy')