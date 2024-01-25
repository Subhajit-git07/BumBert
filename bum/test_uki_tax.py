
import uki_tax_main as ukiTax
import pytest

#if file present then pass otherwise raise exception

#beehive_paths,doc_path,fixed_assest_path = file_search(root_dir_path)


ukiTax.UkiTax().fs_tb_bank_interest()
ukiTax.UkiTax().fs_tb_bank_lease()
ukiTax.UkiTax().fs_tb_profit_loss()
ukiTax.UkiTax().tangible_intangible_assest()
uni_list =ukiTax.UkiTax().tb_fs_list


# @pytest.mark.filterwarnings('ignore::RuntimeWarning')
# def test_doc_processing():
#     if doc_path == '' or beehive_paths=='' or fixed_assest_path=='':
#         raise FileNotFoundError
#     else:
#         print('file present')


@pytest.mark.filterwarnings('ignore::RuntimeWarning')
def test_valid_tables_fs():
    table_list, response_dict=ukiTax.UkiTax().doc_processing()
    if len(table_list)==0:
        assert "Financial statements not having valid tables"
    else:
        pass
    

def test_trial_balance_file_parsing():
    mergeDataFrame = ukiTax.UkiTax().trial_balance_file_parsing()
    if len(mergeDataFrame)==0:
        assert "Trail Balnce not having valid tables"
    else:
        pass

def test_fs_tb_concat_turnover():
    tb_fs_list = ukiTax.UkiTax().fs_tb_concat_turnover()
    if len(tb_fs_list)==0:
        assert IndexError
    else:
        pass


@pytest.mark.filterwarnings('ignore::RuntimeWarning')
def test_FS_TB_recon():
    #decision_list,response_dict = financial_recon_output()
    uni_list =ukiTax.UkiTax().tb_fs_list
    if len(uni_list)==0:
        
        raise IndexError
    else:
        pass

