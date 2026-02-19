import streamlit as st
from google import genai
from google.genai import types
import pathlib
import re
import json
import pandas as pd
import json
import camelot
from datetime import date
import snowflake.connector

curr_date=date.today().strftime('%d-%m-%Y')
client = genai.Client(api_key='AIzaSyD_x63M1gvdMFhcytGuws8meL4IQhu4YVA')
doc1 = pathlib.Path('C:/Users/2149527/OneDrive - Cognizant/Desktop/GenAIPoc/India_Holiday_Calendar_2025.pdf')
doc2 = pathlib.Path('C:/Users/2149527/OneDrive - Cognizant/Desktop/GenAIPoc/Leave_Scope.pdf')
st.title('HR Bot')

def snowflake_connection(sql):
    conn = snowflake.connector.connect(
    user='monopoly22',
    password='8638569740picklU',
    account='VWBIHYL-LJ48583',
    warehouse='compute_wh',
    database='WorkdayPoc',
    schema='WorkdayPoc_Schema')
    query = sql
    cursor = conn.cursor()
    cursor.execute(query)
    df = pd.DataFrame(cursor.fetchall(), columns=[col[0] for col in cursor.description])
    cursor.close()
    conn.close()
    return df
    
def extract_holiday_list():
    pdf_path='India_Holiday_Calendar_2025.pdf'
    tables = camelot.read_pdf(pdf_path, pages="all", flavor="lattice")
    if tables.n == 0:
        print("No tables found in the PDF.")
        return pd.DataFrame()
    #combined_df = pd.concat([table.df for table in tables], ignore_index=True)
    else:
        combined_df=tables[0].df.iloc[2:].copy()
        header_list=['Slno','Holiday_Name','Date','Day','Chennai','Coimbatore','Kochi','Bangalore','Mangalore','Mumbai','Pune','Indore','Ahmedabad','Gurgaon','Noida','Hyderabad','Kolkata','Bhubaneshwar']
        combined_df.columns=header_list
        json_output = combined_df.to_json(orient='records')
        return json_output
        
def Update_leave(data):
    employee_id=data['employee_id']
    type_leave=data['leave_type']
    start_date=data['start_date']
    end_date=data['end_date']
    status="Pending"
    no_of_days_leave=data['count']
    sql=f"""INSERT INTO Leave_Transaction
        (Employee_Id, Start_Date, End_Date, Status, No_of_Days, Leave_Type) 
       VALUES ('{employee_id}', '{start_date}', '{end_date}', 'Pending', '{no_of_days_leave}', '{type_leave}')"""
    df=snowflake_connection(sql)
    sql = f"""UPDATE leave_balance
          SET no_of_leave_available = no_of_leave_available - {no_of_days_leave}
          WHERE EMPLOYEE_ID = '{employee_id}' AND leave_type = '{type_leave}'"""
    df=snowflake_connection(sql)
def check_json(input):
    pattern = r'{[^{}]*}'
    matches = re.findall(pattern, input)
    if matches:
        return matches
    else:
        return None
        
def check_input(input_str):
    pattern = r'{[^{}]*}'
    data = re.findall(pattern, input_str)
    data=data[0]
    try:
        data = json.loads(data.replace("'", '"').replace('\n',''))
        if isinstance(data, dict):
            return data
    except json.JSONDecodeError:
        pass
    return False
    
def emp_check(empid):
    sql="""select EMPLOYEE_ID from Employee_Details"""
    data=snowflake_connection(sql)
    to_list=data['EMPLOYEE_ID'].to_list()
    if empid in to_list:
        return 'valid'
    else:
        return 'invalid'
    
def get_emp_detail(empid):
    sql=f""" WITH Latest_Leave AS (
    SELECT 
        employee_id,
        leave_type,
        MAX(Start_Date) AS start_date,
        MAX(end_date) AS end_date
    FROM 
        Leave_Transaction 
    GROUP BY 
        employee_id, leave_type
        )
        SELECT 
    em.Employee_Id,
    em.gender,
    em.location_name,
    em.Manger_Id,
    lb.No_Of_Leave_Available,
    lb.leave_type,
    ll.start_date,
    ll.end_date
    FROM Employee_Details em
    LEFT JOIN Leave_Balance lb ON lb.Employee_Id = em.Employee_Id
    LEFT JOIN Latest_Leave ll ON ll.employee_id = lb.Employee_Id AND ll.leave_type = lb.leave_type
    WHERE 
     em.Employee_Id='{empid}' """
    user_row=snowflake_connection(sql)
    emp_rec=user_row.to_json(orient='records')
    if user_row.empty:
        return None
    else:
        return emp_rec

def Sugeetion_Generator(user_input):
    sys_ins_data = (
    "You are a suggestion generator. Think step by step and logically. These are the details you need:\n"
    "1)We are creating one chat bot where user can able take leave , plan vacation, policy check ,leave balance check"
    "2) The suggestion should be a question shuould from user's perspective\n"
    "3) Based on the user_input given to you, generate three distinct suggestions.\n"
    "4) Each sugesstion sentence  should not contain muliple choice and should be direct.\n"
    "5) If the last question is about leave then you should provide atleast one suggestion related to apply leave or plan next leave.\n"
    "6) Return the output in the following format: "
    "{{'suggestion1': 'xyz', 'suggestion2': 'xyz', 'suggestion3': 'xyz'}}.\n"
    "Each suggestion must be highly specific to the user input limited to 10 words"
    )
    response = client.models.generate_content(model='gemini-2.5-flash', contents=[user_input], config=types.GenerateContentConfig(temperature=0.1, system_instruction=sys_ins_data))
    if (check_json(response.text)!=None):
        final_output=check_input(response.text)
        return final_output
    else:
        return None       

def insert_into_input(val):
    st.session_state.messages.append({'role': 'user', 'content': val})
    st.session_state.clicked = True

if __name__ == "__main__":
    
    if 'messages' not in st.session_state:
        st.session_state.messages = []
        st.session_state.temp_messages = []
        st.session_state.emp_detail=[]
        st.session_state.cnt=0
        st.session_state.temp_messages.append({'role': 'assistant', 'content': 'Please provide your employee id'})
        st.session_state.clicked = False  
    else:
        pass

    st.markdown("""<style>.st-emotion-cache-1c7yckd{flex-direction: row-reverse;text-align: right;}</style>""",unsafe_allow_html=True)
    
    prompt = st.chat_input("Please type your meassage here")
    if len(st.session_state.messages) == 0:
        for msg in st.session_state.temp_messages:
            with st.chat_message(msg['role']):
                st.write(msg['content'])

        if prompt:
            response = client.models.generate_content(model='gemini-2.5-flash', contents=prompt, config=types.GenerateContentConfig(temperature=0.1, system_instruction='Find and return the only the employee id from the contents'))
            v = emp_check(response.text)
    
            if v=='valid':
                emp= get_emp_detail(response.text)
                st.session_state.emp_detail.append(emp)
                with st.chat_message('user'):
                    st.write(prompt)
                st.session_state.temp_messages.append({'role': 'user', 'content': prompt})
                with st.chat_message('assistant'):
                    st.write('Thank you for the details.. How can I help you today?')
                st.session_state.temp_messages.append({'role': 'assistant', 'content': 'Thank you for the details.. How can I help you today?'})
                st.session_state.messages = st.session_state.temp_messages
                st.session_state.cnt += 1
            elif v=='invalid':
                with st.chat_message('user'):
                    st.write(prompt)
                st.session_state.temp_messages.append({'role': 'user', 'content': prompt})
                with st.chat_message('assistant'):
                    st.write('Please enter a valid employee id')
                st.session_state.temp_messages.append({'role': 'assistant', 'content': 'Please enter a valid employee id'})
            else:
                pass
    else:
        for message in st.session_state.messages:
            with st.chat_message(message['role']):
                st.write(message['content'])

        if prompt or st.session_state.clicked:
            if prompt:
                with st.chat_message('user'):
                    st.write(prompt)
                st.session_state.messages.append({'role': 'user', 'content': prompt})
                st.session_state.cnt += 1
            else:
                pass
              
            df=extract_holiday_list()
            sys_ins = f"You are an HR asistant, think step by step and logically. These are the informations you need: 1) There are one pdfs provided, 1st is leave policy and second one is json that connsist city wise holiday list {df} 2) Leave start year is always 2025. 3) Saturday and Sunday does not count. 4) Leave balance of the employee is: {st.session_state.emp_detail[0]} and don't apply any calculation on it. 5) Give detailed answer based on employee specific leave details and leave policy. 6) Today's date is {curr_date}. 7) If asked for vacation plan then give a detailed explanation without following json. 8) Leave request can not be older than 15 days. 9) If asked for leave and leave type is not given then ask leave type. 10) If a future leave already present on same day then leave can not be applied. 11)If the leave start date and end date fall within an existing leave period, the new leave request should not be allowed.12) Always ask before applying leave and give output in the following format: {{'employee_id':'xyz','leave_type': 'xyz', 'start_date': 'yyyy-mm-dd', 'end_date': 'yyyy-mm-dd', 'count': 00}} only with small explanation, else give an explanation in 30 words. 12) Do not entertain user with any jokes or funny words."

            if len(st.session_state.messages)>=10:
                history=st.session_state.messages[-10:]
            else:
                history=st.session_state.messages
            question=''
            for h in history:
                question=question+'Role: '+h['role']+', Message: '+h['content']+' ,\n'
            response = client.models.generate_content(model='gemini-2.5-flash', contents=[ types.Part.from_bytes(data=doc2.read_bytes(), mime_type='application/pdf'), question], config=types.GenerateContentConfig(temperature=0.1, system_instruction=sys_ins))
            #print(response.text)
            if (check_json(response.text)!=None):
                final_output=check_input(response.text)
                Update_leave(final_output)
                tell="I have placed your leave request"
                emp_id=final_output['employee_id']
                emp_id=get_emp_detail(emp_id)
                st.session_state.emp_detail[0]=emp_id
                st.session_state.messages.append({'role': 'assistant', 'content': tell})
                with st.chat_message('assistant'):
                   st.write(tell)
            else:
                st.session_state.messages.append({'role': 'assistant', 'content': response.text})
                with st.chat_message('assistant'):
                    st.write(response.text)
            if st.session_state.cnt > 0 and prompt:
                sgs_ip='Role: assistant'+', Message: '+st.session_state.messages[-1]['content']+' ,\n'+'Role: user'+', Message: '+prompt
                final_output=Sugeetion_Generator(sgs_ip)
                col1, col2, col3 = st.columns(3)
                with col1:
                    button_clicked1 = st.button(final_output['suggestion1'], on_click= insert_into_input, args=[final_output['suggestion1']])
                with col2:
                    button_clicked2 = st.button(final_output['suggestion2'], on_click= insert_into_input, args=[final_output['suggestion2']])
                with col3:
                    button_clicked3 = st.button(final_output['suggestion3'], on_click= insert_into_input, args=[final_output['suggestion3']])
            elif st.session_state.cnt > 1 and st.session_state.clicked:
                sgs_ip='Role: assistant'+', Message: '+st.session_state.messages[-2]['content']+' ,\n'+'Role: user'+', Message: '+st.session_state.messages[-1]['content']
                final_output=Sugeetion_Generator(sgs_ip)
                st.session_state.clicked = False
                col1, col2, col3 = st.columns(3)
                with col1:
                    button_clicked1 = st.button(final_output['suggestion1'], on_click= insert_into_input, args=[final_output['suggestion1']])
                with col2:
                    button_clicked2 = st.button(final_output['suggestion2'], on_click= insert_into_input, args=[final_output['suggestion2']])
                with col3:
                    button_clicked3 = st.button(final_output['suggestion3'], on_click= insert_into_input, args=[final_output['suggestion3']])
            else:
                pass
        


            


