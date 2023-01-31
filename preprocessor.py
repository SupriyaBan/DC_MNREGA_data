import pandas as pd


INDIA_ISO_CODES = {
'ANDHRA PRADESH':  "IN-AP",
'ARUNACHAL PRADESH':  "IN-AR",
'ASSAM':  "IN-AS",
'BIHAR':  "IN-BR",
'CHATTISGARH':  "IN-CT",
'CHHATTISGARH':  "IN-CT",
'GOA':  "IN-GA",
'GUJARAT':  "IN-GJ",
'HARYANA':  "IN-HR",
'HIMACHAL PRADESH':  "IN-HP",
'JHARKHAND':  "IN-JH",
'JHARKHAND#':  "IN-JH",
'KARNATAKA':  "IN-KA",
'KERALA':  "IN-KL",
'MADHYA PRADESH':  "IN-MP",
'MADHYA PRADESH#':  "IN-MP",
'MAHARASHTRA':  "IN-MH",
'MANIPUR':  "IN-MN",
'MEGHALAYA':  "IN-ML",
'MIZORAM':  "IN-MZ",
'NAGALAND':  "IN-NL",
'NAGALAND#':  "IN-NL",
'ODISHA':  "IN-OR",
'PUNJAB':  "IN-PB",
'RAJASTHAN':  "IN-RJ",
'SIKKIM':  "IN-SK",
'TAMIL NADU':  "IN-TN",
'TELENGANA':  "IN-TG",
'TELANGANA':  "IN-TG",
'TRIPURA':  "IN-TR",
'UTTARAKHAND':  "IN-UT",
'UTTAR PRADESH':  "IN-UP",
'WEST BENGAL':  "IN-WB",
'ANDAMAN AND NICOBAR ISLANDS':  "IN-AN",
'ANDAMAN & NICOBAR ISLANDS':  "IN-AN",
'CHANDIGARH':  "IN-CH",
'DADRA AND NAGAR HAVELI':  "IN-DN",
'DADRA & NAGAR HAVELI':  "IN-DN",
'DADAR NAGAR HAVELI':  "IN-DN",
'DAMAN AND DIU':  "IN-DD",
'DAMAN & DIU':  "IN-DD",
'DELHI':  "IN-DL",
'JAMMU AND KASHMIR':  "IN-JK",
'JAMMU & KASHMIR':  "IN-JK",
'LADAKH':  "IN-LA",
'LAKSHADWEEP':  "IN-LD",
'LAKSHWADEEP':  "IN-LD",
'PONDICHERRY':  "IN-PY",
'PUDUCHERRY':  "IN-PY",
'DADRA AND NAGAR HAVELI AND DAMAN AND DIU':  "IN-DH",
'TELANGANA':  "IN-TG",
'ALL INDIA':  "IN",
'DN HAVELI AND DD':  "IN-DN",
'ANDAMAN AND NICOBAR':  "IN-AN"
}


class MNREGADataLoader:
    def __init__(self, base_url):
        self.base_url = base_url
        self.cols = ['S No.', 'State', 'Total Availablity', 'Unskilled Wage Expenditure', 'Material Expenditure', 'Admin Expenditure',
                     'Other Expenditure', 'Total Actual Exp', 'Actual Balance', 'Unskilled Wage Due', 'Material Due', 'Admin Due', 'Total Due', 
                     'Total Exp including payment due', 'Net Balance', 'Year']
        self.data_list = []
        self.df = None
    
    def download(self):
         #Add the Year's range here
        for yr in range(2019,2023): 
            data = pd.read_html(f"{self.base_url}?lflag=eng&fin_year={yr}-{yr+1}&source=national&labels=labels&Digest=cN96LBEGlHkRAwn+MUntcQ")[4][3:].iloc[:-1]
            #Adding Year column
            data['Year']= f'{yr}' 
            self.data_list.append(data)
        self.df= pd.concat(self.data_list)
        self.df.columns = self.cols
        
    def process(self):
      #Converting the datatypes of numerical column except Year
        for c in self.df.columns[2:]: 
            if c != 'Year':
                self.df[c]= self.df[c].astype('float')
                self.df[c] *= 100000
        #Adding the cumulative columns for Total expenditure Year-wise and State-wise
        self.df['Cum_Expenditure_Year']= self.df.groupby('Year')['Total Actual Exp'].transform('sum')
        self.df['Cum_Expenditure_State'] = self.df.groupby('State')['Total Actual Exp'].transform('sum')
        

        self.df["State"] = self.df["State"].apply(lambda x: INDIA_ISO_CODES[x])

    
    def save(self, filename='mnrega_data.csv'):
        self.df.to_csv(filename, index=False)
        

def main():
    """Runs the program."""
    loader = MNREGADataLoader("https://mnregaweb4.nic.in/netnrega/Citizen_html/financialstatement.aspx")
    loader.download()
    loader.process()
    loader.save()


if __name__ == '__main__':
    main()
