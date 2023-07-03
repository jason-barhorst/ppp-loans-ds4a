#!/usr/bin/env python
# coding: utf-8

# # PPP FOIA Data Cleaning
# Data files can be downloaded from https://data.sba.gov/dataset/ppp-foia

import os
from pathlib import Path

import pandas as pd
import uszipcode

search = uszipcode.SearchEngine()

# ## Combine & Load Data
data_dir = Path("./data/PPP-FOIA/")
cleaned_data_dir = data_dir / "cleaned"
cleaned_data_dir.mkdir(exist_ok=True)


# Running this with all data uses ~9 Gb of RAM
nrows: int | None = None
filenames = os.listdir(data_dir)
filenames = [str(data_dir / f) for f in filenames if f.endswith(".csv")]
print(f"Loading {len(filenames)} data files.")
df = pd.concat(map(lambda x: pd.read_csv(x, nrows=nrows, low_memory=False), filenames))


# # Data Cleaning:
# ## Drop Columns
drop_columns = [
    "SBAOfficeCode",  # Originating office. Not used
    "ProcessingMethod",  # PPP or PPS? Not used
    "SBAGuarantyPercentage",  # Every value is 100%
    "FranchiseName",  # 98.7% Missing value
    "ServicingLenderLocationID",  # Not concerned with servicing lender
    "ServicingLenderName",
    "ServicingLenderAddress",
    "ServicingLenderCity",
    "ServicingLenderState",
    "ServicingLenderZip",
    "HubzoneIndicator",  # Not used
    "LMIIndicator",  # What is it? Not used
    "BusinessAgeDescription",  # Not used
    "ProjectCity",  # Project data. Not used
    "ProjectCountyName",
    "ProjectState",
    "ProjectZip",
    "CD",  # Project Congressional District. Not used
    "Race",  # Not used
    "Ethnicity",  # Not used
    "UTILITIES_PROCEED",  # PROCEED Data not used
    "PAYROLL_PROCEED",
    "MORTGAGE_INTEREST_PROCEED",
    "RENT_PROCEED",
    "REFINANCE_EIDL_PROCEED",
    "HEALTH_CARE_PROCEED",
    "DEBT_INTEREST_PROCEED",
    "OriginatingLenderLocationID",  # Not used
    "Gender",  # Not used
    "Veteran",  # Not used
    "NonProfit",  # Not used
]

df = df.drop(columns=drop_columns, axis=1)

# # Clean Reamining Columns:

# ## LoanNumber
# 100% distinct. No missing values. No actions required.

# ## DateApproved
# No missing values. Convert to date type.
df.DateApproved = pd.to_datetime(df.DateApproved)


# ## BorrowerName
# 58 Missing values. Reviewed the data and other fields were provided. Changing name to 'name not provided' to clear the nulls. Converting to categorical.
df.BorrowerName = df.BorrowerName.fillna("missing borrower name")
df.BorrowerName = pd.Categorical(df.BorrowerName)


# ## BorrowerAddress
# 214 missing values. Of these, 191 do not provide city or zip. Dropping 191 rows, filling the rest with 'missing address'
df = df.dropna(subset=["BorrowerAddress", "BorrowerCity", "BorrowerZip"], how="all")
df.BorrowerAddress = df.BorrowerAddress.fillna("missing address")


# ## BorrowerCity
# Using uszipcode library to fill in missing city. If the zip and city are not available, we do not know the location of the bank. Check for bank names.
def zip2city(zip_code):
    if pd.isnull(zip_code):
        return None
    else:
        return search.by_zipcode(zip_code).major_city


df.loc[df.BorrowerCity.isnull(), "BorrowerCity"] = df[df.BorrowerCity.isnull()][
    "BorrowerZip"
].apply(zip2city)


# ## BorrowerState
# Using uszipcode library to fill in missing states.
def zip2state(zip_code):
    # Only use 5 digit zip
    if pd.isnull(zip_code):
        return None
    zip_code = zip_code.split("-")[0]
    return search.by_zipcode(zip_code).state_abbr


df.loc[df.BorrowerState.isnull(), "BorrowerState"] = df[
    df.BorrowerState.isnull()
].BorrowerZip.apply(zip2state)


# ## BorrowerZip
# 5 missing values remain. Fill in with uszipcode search by city, state.
city_state_w_null_zip = list(
    zip(
        df[df.BorrowerZip.isnull()].BorrowerCity,
        df[df.BorrowerZip.isnull()].BorrowerState,
    )
)
# df.BorrowerZip[df.BorrowerZip.isnull()] = [search.by_city_and_state(cs[0], cs[1]) for cs in city_state_w_null_zip]
zipcodes = [
    search.by_city_and_state(cs[0], cs[1])[0].zipcode for cs in city_state_w_null_zip
]
df.loc[df.BorrowerZip.isnull(), "BorrowerZip"] = zipcodes


# ## Loan Status Date:
# Missing 565782 values (4.9% of values). Every loan missin Loan Status Date has a Loan Status of "Exemption 4." Loans considered exempt, were those whose status was protected by FOIA’s Exemption 4, which is specifically intended to protect “submitters who are required to furnish commercial or financial information to the government by safeguarding them from the competitive disadvantages that could result from disclosure.”
#
# Leaving the missing data as null


# ## Loan Status:
# No missing values

# ## Term
# No issues.

# ## Initial Approval Amount
# 20 values less than or equal to zero. Each of these loans had an amount for Current Approval Amount and had 0 for Undisbursed Amount. Setting the value of Initial Approval Amount to Current Approval Amount.
df.loc[df.InitialApprovalAmount <= 0.0, "InitialApprovalAmount"] = df[
    "CurrentApprovalAmount"
][df.InitialApprovalAmount <= 0.0]


# ## Current Approval Amount
# No missing values.

# ## Undisbursed Amount
# Missing 1171 values. 99.9% of values in this field are zero. Setting missing values to zero (already skew).
df.loc[df.UndisbursedAmount.isnull(), "UndisbursedAmount"] = 0


# ## Jobs Reported
# One negative value and 8 missing. 210 Zeros (likely people not counting themselves). Setting missing / negative values to 1.
df.loc[df.JobsReported.isnull(), "JobsReported"] = 1
df.loc[df.JobsReported < 0, "JobsReported"] = 1


# ## NAICS Code
# Missing 132292 (1.2%) values. Converting to datatype string. Leaving null fields.
df.NAICSCode = df.NAICSCode.astype(str)


# ## Business Type
# Missing 2233 values. Many business names contain "LLC" or "INC" updating these. Remaining 950 null values converted to 'Other'
df.loc[
    df.BusinessType.isnull() & df.BorrowerName.str.contains("INC"), "BusinessType"
] = "Corporation"
df.loc[
    df.BusinessType.isnull() & df.BorrowerName.str.contains("LLC"), "BusinessType"
] = "Limited  Liability Company(LLC)"
df.loc[df.BusinessType.isnull(), "BusinessType"] = "Other / Unknown"


# ## Originating Lender (Lender, City, State)
# No missing values.


# ## Forgiveness Amount
# Missing 914809 values. Setting forgiveness amount to zero for missing values.
df.loc[df.ForgivenessAmount.isnull(), "ForgivenessAmount"] = 0

# Close search connection
search.close()

# ## Export Cleaned Data (one file)
df.to_csv(cleaned_data_dir / "ppp_foia_cleaned.csv", index=False)


# ## Transform Datatypes
# dtypes can be applied when loading data. The following definition constains all columns, but requires field updates.
# TODO: Select correct dtypes and apply datetime transform as needed. Once ready, use these transforms to create/load the combined file.
dtype = {
    "LoanNumber": int,  # Loan Number (unique identifier)
    "DateApproved": str,  # Loan Funded Date
    "SBAOfficeCode": str,  # SBA Origination Office Code
    "ProcessingMethod": str,  # Loan Delivery Method (PPP for first draw; PPS for second draw)
    "BorrowerName": str,  # Borrower Name
    "BorrowerAddress": str,  # Borrower Street Address
    "BorrowerCity": str,  # Borrower City
    "BorrowerState": str,  # Borrower State
    "BorrowerZip": str,  # Borrower Zip Code
    "LoanStatusDate": str,  # Loan Status Date - Loan Status Date is  blank when the loan is disbursed but not Paid In Full or Charged Off
    "LoanStatus": int,  # Loan Status Description - Loan Status is replaced by 'Exemption 4' when the loan is disbursed but not Paid in Full or Charged Off
    "Term": str,  # Loan Maturity in Months
    "SBAGuarantyPercentage": str,  # SBA Guaranty Percentage
    "InitialApprovalAmount": str,  # Loan Approval Amount(at origination)
    "CurrentApprovalAmount": str,  # Loan Approval Amount (current)
    "UndisbursedAmount": str,  # Undisbursed Amount
    "FranchiseName": str,  # Franchise Name
    "ServicingLenderLocationID": str,  # Lender Location ID (unique identifier)
    "ServicingLenderName": str,  # Servicing Lender Name
    "ServicingLenderAddress": str,  # Servicing Lender Street Address
    "ServicingLenderCity": str,  # Servicing Lender City
    "ServicingLenderState": str,  # Servicing Lender State
    "ServicingLenderZip": str,  # Servicing Lender Zip Code
    "RuralUrbanIndicator": str,  # Rural or Urban Indicator (R/U)
    "HubzoneIndicator": str,  # Hubzone Indicator (Y/N)
    "LMIIndicator": str,  # LMI Indicator (Y/N)
    "BusinessAgeDescription": str,  # Business Age Description
    "ProjectCity": str,  # Project City
    "ProjectCountyName": str,  # Project County Name
    "ProjectState": str,  # Project State
    "ProjectZip": str,  # Project Zip Code
    "CD": str,  # Project Congressional District
    "JobsReported": str,  # Number of Employees
    "NAICSCode": str,  # NAICS 6 digit code
    "Race": str,  # Borrower Race Description
    "Ethnicity": str,  # Borrower Ethnicity Description
    "UTILITIES_PROCEED": str,  # Note: Proceed data is lender reported at origination.  On the PPP application the proceeds fields were check boxes.
    "PAYROLL_PROCEED": str,  # Note: Proceed data is lender reported at origination.  On the PPP application the proceeds fields were check boxes.
    "MORTGAGE_INTEREST_PROCEED": str,  # Note: Proceed data is lender reported at origination.  On the PPP application the proceeds fields were check boxes.
    "RENT_PROCEED": str,  # Note: Proceed data is lender reported at origination.  On the PPP application the proceeds fields were check boxes.
    "REFINANCE_EIDL_PROCEED": str,  # Note: Proceed data is lender reported at origination.  On the PPP application the proceeds fields were check boxes.
    "HEALTH_CARE_PROCEED": str,  # Note: Proceed data is lender reported at origination.  On the PPP application the proceeds fields were check boxes.
    "DEBT_INTEREST_PROCEED": str,  # Note: Proceed data is lender reported at origination.  On the PPP application the proceeds fields were check boxes.
    "BusinessType": str,  # Business Type Description
    "OriginatingLenderLocationID": str,  # Originating Lender ID (unique identifier)
    "OriginatingLender": str,  # Originating Lender Name
    "OriginatingLenderCity": str,  # Originating Lender City
    "OriginatingLenderState": str,  # Originating Lender State
    "Gender": str,  # Gender Indicator
    "Veteran": str,  # Veteran Indicator
    "NonProfit": str,  # 'Yes' if Business Type = Non-Profit Organization or Non-Profit Childcare Center or 501(c) Non Profit
    "ForgivenessAmount": str,  # Forgiveness Amount
    "ForgivenessDate": str,  # Forgiveness Paid Date
}
