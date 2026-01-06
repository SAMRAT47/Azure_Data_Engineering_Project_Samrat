class reusable:

    
    def dropColumns(self,df,columns):
        df = df.drop(*columns)
        return df