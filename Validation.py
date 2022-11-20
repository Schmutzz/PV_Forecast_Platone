import pandas as pd
from sklearn.metrics import r2_score


class Validation:
    def __init__(self, df):
        self.df = df

    def calcR2_score(self, df=None, realValCol=None, forecastCol=None):
        # input
        if type(df) == type(None):
            tempDf = self.df
        else:
            tempDf = df

        # function
        # series need to be of same length
        tempDf['R2'] = r2_score(tempDf[realValCol], tempDf[forecastCol])

        # output
        if type(df) == type(None):
            self.df = tempDf
        else:
            return tempDf


if __name__ == "__main__":
    data = {'forecast': [0, 1, 2, 3, 4, 3, 2, 1, 0, 0], 'realVal': [0, 10, -3, 2, -4, 30, 1, 2, 1, 0]}
    df_test = pd.DataFrame(data)
    val = Validation(df=df_test)
    val.calcR2_score(realValCol='realVal', forecastCol='forecast')
    print(val.df)
