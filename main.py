import functions
import db
import datetime

# Start

# 데이터베이스 커넥션 생성
dbc = db.get_connection()

# 프로세스 실행중 여부 확인
def main_process(type):
    if functions.is_script_running(type):
        exit()

    # target date 생성
    target_date = functions.get_target_date(type, dbc, datetime.date.today())

    if functions.is_target_date_over_today(target_date):
        exit()
    else:
        while not functions.is_target_date_over_today(target_date):
            functions.write_target_date_information(type, target_date)
            print('target_date')
            print(target_date)

            dmc_list_from_bulkserver = functions.get_dmc_list_from_bulk_server(type, target_date)
            if dmc_list_from_bulkserver:
                dmc_list_from_db = functions.get_dmc_list_from_db(type, target_date, dbc)
                # dmc_list delta 확인
                dmc_list_to_be_checked = list(set(dmc_list_from_bulkserver).difference(set(dmc_list_from_db)))
                if dmc_list_to_be_checked:
                    end_force_values = {}
                    for dmc in dmc_list_to_be_checked:
                        # 실제 해당 dmc의 PD.csv 파일이 있는지 확인
                        if functions.is_pdfile_exists(type, target_date, dmc):
                            try:
                                results = functions.preprocess(type, target_date, dmc)
                            except:
                                continue
                            functions.save_result_at_db(type, target_date, dmc, results, dbc)
                            end_force_values = functions.preprocess_and_save_only_end_force_value(results, end_force_values)
                    
                    total_end_force_value = 0
                    avg_end_force_values = {}
                    avg_end_force_values = functions.calc_avg_end_force_values(avg_end_force_values, end_force_values, total_end_force_value)
                    functions.save_avg_end_force_values(type, target_date, avg_end_force_values, dbc)
                if functions.is_target_date_today(target_date):
                    exit() # restart by scheduler
                else:
                    functions.write_complete_information(type, target_date)
                    target_date = target_date + datetime.timedelta(days=1)
                    functions.update_last_mfd_at_file(type, target_date)
            else:
                if functions.is_target_date_today(target_date):
                    exit() # restart by scheduler
                else:
                    functions.write_complete_information(type, target_date)
                    target_date = target_date + datetime.timedelta(days=1)
                    functions.update_last_mfd_at_file(type, target_date)
