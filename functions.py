import datetime
from glob import glob
import os

from sqlalchemy import true
import pd_preprocessing

BASE_BULK_SERVER_PATH = "\\\\dae0fb01.apac.bosch.com\\EDC\\Common\\FA3"

def __create_partial_path_for_bulkserver(type):
    if type == 'bracket':
        return BASE_BULK_SERVER_PATH + "\\Clinching_Bracket"
    elif type == 'housing':
        return BASE_BULK_SERVER_PATH + "\\Clinching_Housing"
    else:
        raise Exception("처리할 수 없는 타입")

def __create_complete_path_for_bulkserver(type, target_date):
    return __create_partial_path_for_bulkserver(type) + target_date.strftime("\\%Y\\%m\\%d")

def __create_target_date_file_path(type):
    file_path = f'{type}_target_date.txt'
    return file_path

def is_script_running(type):
    return None

def get_target_date(type, dbc, default_date):
    target_date_from_file = None
    file_path = __create_target_date_file_path(type)
    if os.path.isfile(file_path):
        with open(file_path, 'r') as file:        
            if file:
                line = file.readline()
                if line:
                    target_date_from_file = datetime.datetime.strptime(line, '%Y-%m-%d').date()
                    
    target_date_from_db = None
    if not target_date_from_file:
        type_upper_case = type.upper()

        with dbc.cursor() as cursor:
            cursor.execute(f"SELECT TOP(1) [mfd] FROM clinching_result WHERE clinching_type = '{type_upper_case}' ORDER BY mfd")
            row = cursor.fetchone()
            if row:
                target_date_from_db = row[0].date()

    target_date = default_date
    if target_date_from_file:
        target_date = target_date_from_file
    elif target_date_from_db:
        target_date = target_date_from_db
    
    return target_date

def is_target_date_over_today(target_date):
    if(target_date > datetime.date.today()):
        return True
    else:
        return False
    
def is_target_date_folder_exists(target_date):
    folder_path = __create_complete_path_for_bulkserver(type, target_date)
    return os.path.exists(folder_path)

def is_target_date_today(target_date):
    if(target_date == datetime.date.today()):
        return True
    else:
        return False

def get_dmc_list_from_bulk_server(type, target_date):
    target_path = __create_complete_path_for_bulkserver(type, target_date)
    dmc_list = glob(target_path + "\\*")
    dmc_list_from_bulkserver = list(map(lambda x: x.split('\\')[-1], dmc_list))
    return dmc_list_from_bulkserver

def get_dmc_list_from_db(type, target_date, dbc):
    type_upper_case = type.upper()
    with dbc.cursor() as cursor:
        cursor.execute("""
            SELECT
                dmc
            FROM
                clinching_dmc
            WHERE
                mfd BETWEEN %s AND %s
                AND clinching_type = %s
        """, (
            target_date.strftime("%Y-%m-%d 00:00:00"),
            target_date.strftime("%Y-%m-%d 23:59:59"),
            type_upper_case        
            ))

        rows = cursor.fetchall()
        return list(map(lambda x: x[0], rows))

def is_pdfile_exists(type, target_date, dmc):
    target_path = __create_complete_path_for_bulkserver(type, target_date) + '\\' + dmc + '\\' + dmc + 'PD.csv'
    if os.path.isfile(target_path):
        return True
    else:
        return False

def preprocess(type, target_date, dmc):
    pd_file_path =__create_complete_path_for_bulkserver(type, target_date) + '\\' + dmc + '\\' + dmc + 'PD.csv'

    titles_and_values = {}

    for line in open(pd_file_path):
        info = pd_preprocessing.LineInfo(line)
        if info.title in pd_preprocessing.LineInfo.types.keys():
            titles_and_values[info.title] = info.values

    if (type == 'housing'):
        if not (len(titles_and_values['Clinch point']) == 11):
            raise Exception("Wrong clinching-type")

    results = {}

    def confirm_curve_result(cp, title, value):
        if cp not in results:
            results[cp] = {}
        results[cp][title] = value

    def confirm_endforce_value_and_result(cp, title, value, index):
        value = float(value)
        low_limit = float(titles_and_values['EndForce_LoLim'][index])
        up_limit = float(titles_and_values['EndForce_UpLim'][index])
        
        results[cp]['EndForce_Value'] = value
    
        if(value > low_limit and value < up_limit):
            results[cp]['EndForce_Result'] = True
        else:
            results[cp]['EndForce_Result'] = False

    def confirm_endposition_value_and_result(cp, title, value, index):
        value = float(value)
        low_limit = float(titles_and_values['EndPosition_LoLim'][index])
        up_limit = float(titles_and_values['EndPosition_UpLim'][index])
        
        results[cp]['EndPosition_Value'] = value

        if(value > low_limit and value < up_limit):
            results[cp]['EndPosition_Result'] = True
        else:
            results[cp]['EndPosition_Result'] = False

    def confirm_gradient_result(cp, title, value):
        results[cp][title] = value

    def confirm_dmc_result(results):   
        for cp in results:
            result = results[cp]['operation_result']
            if not result:
                results['total_operation_result'] = False
                return results
                
        results['total_operation_result'] = True

    clamping_positions = titles_and_values['Clinch point']

    for (index, cp) in enumerate(clamping_positions):
        for title in titles_and_values:
            if(title == 'Curve_Result'):
                confirm_curve_result(cp, title, titles_and_values[title][index])
            if(title == 'EndForce_Value [N]'):
                confirm_endforce_value_and_result(cp, title, titles_and_values[title][index], index)
            if(title == 'EndPosition_Value'):
                confirm_endposition_value_and_result(cp, title, titles_and_values[title][index], index)
            if(title == 'Gradient_Result'):
                confirm_gradient_result(cp, title, titles_and_values[title][index])
    
    for cp in results:
        if((results[cp]['Curve_Result'] == True) 
        and (results[cp]['EndForce_Result'] == True) 
        and (results[cp]['EndPosition_Result'] == True) 
        and (results[cp]['Gradient_Result'] == True)):
            results[cp]['operation_result'] = True
        else:
            results[cp]['operation_result'] = False
    
    confirm_dmc_result(results)

    return results

def preprocess_and_save_only_end_force_value(results, end_force_values):
    for cp in results:       
        if(cp.startswith('CP')):
            end_force_value = results[cp]['EndForce_Value']
            if not (cp in end_force_values):
                end_force_values[cp] = []
            end_force_values[cp].append(end_force_value)

    return end_force_values

def calc_avg_end_force_values(avg_end_force_values, end_force_values, total_end_force_value):
    for cp in end_force_values:
        nums = len(end_force_values[cp])

        for value in end_force_values[cp]:
            total_end_force_value = total_end_force_value + value

        avg_end_force_values[cp] = total_end_force_value / nums
        total_end_force_value = 0

    return avg_end_force_values

def save_avg_end_force_values(type, target_date, avg_end_force_values, dbc):
    type_upper_case = type.upper()

    with dbc.cursor() as cursor:
        for cp in avg_end_force_values:
            avg_end_force = avg_end_force_values[cp]

            query = """
                INSERT INTO clinching_avg_end_force
                (
                    clinching_type,
                    mfd,
                    cp,
                    average_end_force_value,
                    created_at      
                )
                VALUES (%s, %s, %s, %s, NOW())
            """
            cursor.execute(query, (
                type_upper_case,
                target_date.strftime("%Y-%m-%d 00:00:00"),
                cp,
                avg_end_force
            ))
            dbc.commit()

def __save_dmc_at_db(type_upper_case, target_date, dmc, results, dbc):
    with dbc.cursor() as cursor:
        query = """
            INSERT INTO clinching_dmc
            (
                dmc,
                clinching_type,
                mfd,
                created_at,
                updated_at,
                operation_result      
            )
            VALUES (%s, %s, %s, NOW(), NOW(), %s)
        """
        cursor.execute(query, (
            dmc,
            type_upper_case,
            target_date.strftime("%Y-%m-%d 00:00:00"),
            results['total_operation_result']
        ))

        cursor.execute("SELECT @@IDENTITY")
        dmc_id = cursor.fetchone()[0]
        dbc.commit()

    return dmc_id

def save_result_at_db(type, target_date, dmc, results, dbc):
    type_upper_case = type.upper()
    dmc_id = __save_dmc_at_db(type_upper_case, target_date, dmc, results, dbc)

    for position in results:
        if 'total' not in position:
            with dbc.cursor() as cursor:
                query = """
                    INSERT INTO clinching_cp
                    (
                        dmc_id,
                        cp,
                        curve_result,
                        end_position_value,
                        end_force_value,
                        gradient_result,
                        created_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, NOW())
                """
                cursor.execute(query, (
                    dmc_id,
                    position,
                    results[position]['Curve_Result'],
                    results[position]['EndPosition_Value'],
                    results[position]['EndForce_Value'],
                    results[position]['Gradient_Result']
                ))
                dbc.commit()

def update_last_mfd_at_file(type, target_date):
    target_date = target_date.strftime("%Y-%m-%d")
    file_path = __create_target_date_file_path(type)
    if os.path.isfile(file_path):
        with open(file_path, 'w') as file:        
            if file:
                file.write(target_date)