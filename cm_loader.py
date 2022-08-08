import json
import dateutil.parser
import datetime 
from typing import Any, Dict, List, Tuple
from random import randint
import sqlalchemy as sa
from sqlalchemy.orm import Session
from sqlalchemy.sql.functions import func
from sqlalchemy.sql.expression import or_, and_
from migration import get_microservice_session, get_micro_session
from migration import db_model, EVENT_SCHEMA_VERSION, cc
import pandas as pd
import csv_loader as csvl
import comments_config_constants as cfg_const
import comments_config as cfg
import comments.model.label_aggregate as lag

USER_ID_MAP = None
DOC_ID_MAP = None

def create_label_event_data(cp_id: int, user_id: int, request_verb: str,
        label_id: int=None, request_body: Dict=None, event_col_id: Any=None, 
        created: str=datetime.datetime.utcnow().isoformat(), session: Session=None):
 
    label_event_data = db_model.LabelEvent.serialize_event(cp_id, user_id, request_verb, None,
                    request_body, event_col_id, created)
    label_event = db_model.LabelEvent(cp_id=cp_id, user_id=user_id, request_verb=request_verb,
                    schema_ver=EVENT_SCHEMA_VERSION, event_data=label_event_data)
    session.add(label_event)

def get_request_label_dict(label_name, label_description, is_global: bool=True, user_id: st):
    request_label_dict = {}
    request_label_dict[LABEL_JSON.LABEL_NAME.value] = label_name
    request_label_dict[LABEL_JSON.GLOBAL_STATUS.value] = is_global
    request_label_dict[LABEL_JSON.LABEL_DESCRIPTION.value] = label_description 
        # document_ids = request_label_dict.get(LABEL_JSON.DOC_ID_LIST.value, None)
    ds.handle_duplicate_label_name(cp_id, label_name, user_id, self.session)
    return request_label_dict

def serialize_label_table(micro_session: Session, data_frame: pd.DataFrame):
    all_labels = data_frame
    current_labels = micro_session.query(db_model.Label.label_name).where(db_model.Label.cp_id == 8) \
        .filter(sa.or_(db_model.Label.label_name.like('Question%[0-9]'), db_model.Label.label_name.like('Commenter:%'))) \
        .order_by(db_model.Label.label_name).all()
    for index, label in all_labels.iterrows():
        label_uuid = None #str(label.label_uuid)
        request_body = {cc.LABEL_JSON.LABEL_NAME.value: label["label_name"],
                        cc.LABEL_JSON.LABEL_DESCRIPTION.value: label["label_description"], 
                        cc.LABEL_JSON.GLOBAL_STATUS.value: label["is_global"]}
        if not label["user_id"] or label["user_id"] == 'None':
            new_user_id = None
        else:
            new_user_id = label["user_id"] # USER_ID_MAP[label["user_id"]] if label["user_id"] else None

        request_verb = 'POST'
        create_label_event_data(label["cp_id"], new_user_id, request_verb, label_uuid, 
                            request_body, label_uuid, datetime.datetime.utcnow().isoformat(), micro_session)
        # docs_label = mono_session.execute(f'''SELECT id, doc_id, user_id, deleted, created, 
        #                             last_modified from cra_document_label
        #                             where label_uuid='{label_uuid}' order by created, id''').all()
        # _serialize_document_labels(label["cp_id"], label_uuid, docs_label, micro_session)
        # if label.created != label.last_modified:
        #     request_verb = 'PUT'
        #     create_label_event_data(label.cp_id, new_user_id, request_verb, label_uuid,
        #                             request_body, label_uuid, label.last_modified.isoformat(),micro_session)
        # if label.deleted:
        #     request_verb = 'DELETE'
        #     create_label_event_data(label.cp_id, new_user_id, request_verb, label_uuid,
        #                             None, label_uuid, label.last_modified.isoformat(), micro_session)
        micro_session.commit()

# def _serialize_document_labels(cp_id: int, label_uuid: str, doc_labels: List, micro_session: Session):
#     for doc_label in doc_labels:
#         request_verb = 'POST'
#         new_doc_id = DOC_ID_MAP[doc_label.doc_id]
#         request_body = {cc.LABEL_JSON.DOC_ID_LIST.value: [new_doc_id]}
#         new_user_id = USER_ID_MAP[doc_label.user_id] if doc_label.user_id else None
#         create_label_event_data(cp_id, new_user_id, request_verb, label_uuid, 
#                             request_body, label_uuid, doc_label.created.isoformat(), micro_session)
#         if doc_label.deleted:
#             request_verb = 'DELETE'
#             create_label_event_data(cp_id, new_user_id, request_verb, label_uuid,
#                             request_body, label_uuid, doc_label.last_modified.isoformat(), micro_session)

def deserialize_label_table(micro_session: Session) -> Tuple[List[Dict], List[Dict]]:
    generated_label_list = []
    generated_doc_label_list = []
    label_events = micro_session.query(db_model.LabelEvent).order_by(db_model.LabelEvent.id).all()
    for label_event in label_events:
        event_data = json.loads(label_event.event_data)
        request_verb = event_data[cc.EVENT_TABLE_META.REQUEST_VERB.value] 
        request_data = event_data[cc.EVENT_TABLE_META.REQUEST_BODY.value]
        label_uuid = event_data[cc.EVENT_TABLE_META.EVENT_COL_ID.value]
        if request_verb == 'POST':
            if cc.LABEL_JSON.LABEL_NAME.value in request_data:
                new_row = {cc.EVENT_TABLE_META.CP_ID.value: event_data[cc.EVENT_TABLE_META.CP_ID.value],
                    cc.LABEL_JSON.LABEL_NAME.value: request_data[cc.LABEL_JSON.LABEL_NAME.value],
                    cc.LABEL_JSON.LABEL_DESCRIPTION.value: request_data[cc.LABEL_JSON.LABEL_DESCRIPTION.value], 
                    cc.EVENT_TABLE_META.CP_ID.value: event_data[cc.EVENT_TABLE_META.CP_ID.value],
                    cc.LABEL_JSON.GLOBAL_STATUS.value: request_data[cc.LABEL_JSON.GLOBAL_STATUS.value], 
                    cc.EVENT_TABLE_META.CREATED_DATE.value: event_data[cc.EVENT_TABLE_META.CREATED_DATE.value],
                    cc.EVENT_TABLE_META.LAST_MODIFIED.value: event_data[cc.EVENT_TABLE_META.CREATED_DATE.value], 
                    cc.LABEL_EVENT_JSON.LABEL_UUID.value: label_uuid,
                    cc.EVENT_TABLE_META.USER_ID.value: label_event.user_id}
                generated_label_list.append(new_row)
            else:
                new_row = {cc.LABEL_JSON.DOC_ID.value: request_data[cc.LABEL_JSON.DOC_ID_LIST.value][0],
                    cc.LABEL_EVENT_JSON.LABEL_UUID.value: label_uuid,
                    cc.EVENT_TABLE_META.CP_ID.value: event_data[cc.EVENT_TABLE_META.CP_ID.value],
                    cc.EVENT_TABLE_META.CREATED_DATE.value:event_data[cc.EVENT_TABLE_META.CREATED_DATE.value],
                    cc.EVENT_TABLE_META.LAST_MODIFIED.value: event_data[cc.EVENT_TABLE_META.CREATED_DATE.value],
                    cc.EVENT_TABLE_META.USER_ID.value: label_event.user_id}
                generated_doc_label_list.append(new_row)
        elif request_verb == 'PUT':
            index = next((i for i, label in enumerate(generated_label_list) if label[cc.LABEL_EVENT_JSON.LABEL_UUID.value] == label_uuid), None)
            label_to_update = generated_label_list[index]
            new_row = {cc.EVENT_TABLE_META.CP_ID.value: label_to_update[cc.EVENT_TABLE_META.CP_ID.value], 
                cc.LABEL_JSON.LABEL_NAME.value: request_data[cc.LABEL_JSON.LABEL_NAME.value],
                cc.LABEL_JSON.LABEL_DESCRIPTION.value: request_data[cc.LABEL_JSON.LABEL_DESCRIPTION.value],
                cc.LABEL_JSON.GLOBAL_STATUS.value: request_data[cc.LABEL_JSON.GLOBAL_STATUS.value], 
                cc.EVENT_TABLE_META.CREATED_DATE.value: label_to_update[cc.EVENT_TABLE_META.CREATED_DATE.value],
                cc.EVENT_TABLE_META.LAST_MODIFIED.value: event_data[cc.EVENT_TABLE_META.CREATED_DATE.value], 
                cc.LABEL_EVENT_JSON.LABEL_UUID.value: label_to_update[cc.LABEL_EVENT_JSON.LABEL_UUID.value],
                cc.EVENT_TABLE_META.USER_ID.value: label_event.user_id}
            generated_label_list[index] = new_row
        elif request_verb == 'DELETE':
            if request_data is not None:
                doc_id = request_data[cc.LABEL_JSON.DOC_ID_LIST.value][0]
                index = next((i for i, doc_label in enumerate(generated_doc_label_list) 
                            if doc_label[cc.LABEL_JSON.DOC_ID.value] == doc_id 
                            and doc_label[cc.LABEL_EVENT_JSON.LABEL_UUID.value] == label_uuid), None)
                generated_doc_label_list.pop(index) if index is not None else generated_doc_label_list
            else:
                index = next((i for i, label in enumerate(generated_label_list)
                             if label[cc.LABEL_EVENT_JSON.LABEL_UUID.value] == label_uuid), None)
                generated_label_list.pop(index)
                doc_label_idx_lst = [index for index, doc_label in enumerate(generated_doc_label_list)
                                 if doc_label[cc.LABEL_EVENT_JSON.LABEL_UUID.value] == label_uuid]
                if len(doc_label_idx_lst) > 0:
                    generated_doc_label_list = [doc_label for doc_label in generated_doc_label_list 
                                    if doc_label[cc.LABEL_EVENT_JSON.LABEL_UUID.value] != label_uuid]


    return generated_label_list, generated_doc_label_list


def populate_into_new_tables(generated_label_lst: List[Dict], generated_doc_label_lst: List[Dict],micro_session: Session) -> Dict[str, int]:
    label_id_map = {}
    current_labels = micro_session.query(db_model.Label.label_name) \
        .filter(sa.and_(db_model.Label.cp_id == 8 \
            , sa.or_(db_model.Label.label_name.like('Question%[0-9]'), db_model.Label.label_name.like('Commenter%')))) \
                .order_by(db_model.Label.label_name).all()
    # for col in [r[0] for r in current_labels]: print(col)
    for label in generated_label_lst:        
        if  label['label_name'] not in [r[0] for r in current_labels]: # \
            label_uuid = label[cc.LABEL_EVENT_JSON.LABEL_UUID.value]
            label_seq_value = micro_session.execute(db_model.LabelSequence)
            new_user_id = label[cc.EVENT_TABLE_META.USER_ID.value]
            label_id_map[label_uuid] = label_seq_value
            new_label = db_model.Label(id=label_seq_value, cp_id=label[cc.EVENT_TABLE_META.CP_ID.value], 
                    label_name=label[cc.LABEL_JSON.LABEL_NAME.value], 
                    label_description=label[cc.LABEL_JSON.LABEL_DESCRIPTION.value],
                    user_id=new_user_id, is_global=True, # label[cc.LABEL_JSON.GLOBAL_STATUS.value],
                    created=dateutil.parser.parse(label[cc.EVENT_TABLE_META.CREATED_DATE.value]), 
                    last_modified=dateutil.parser.parse(label[cc.EVENT_TABLE_META.LAST_MODIFIED.value]))
            micro_session.add(new_label)

    return label_id_map


def main():
    global USER_ID_MAP, DOC_ID_MAP


    session = get_micro_session(cfg_const.ENVIRONMENT_TYPE.LOCAL_DB.value)

    columns = ["cp_id", "label_name", "description","is_global","user_id"]
    colfmt = ['{0}','Q {0} - {0}','{0}','{0}','{0}']    
    df_questions = csvl.read_csv_to_list_multiline(csvl.QUESTIONS, columns, colfmt, None)
    colfmt = ['{0}','{0}','{0}','{0}','{0}']
    df_commenters = csvl.read_csv_to_list_multiline(csvl.COMMENTERS, columns, colfmt, None)
    # df_merged = pd.concat([df_questions, df_commenters], ignore_index=True, sort=False)
    # # load Questions file
    # serialize_label_table(session, df_merged)
    # label_lst, doc_label_lst = deserialize_label_table(session)
    # label_id_map = populate_into_new_tables(label_lst, doc_label_lst, session)
    # update_label_id_mapping(label_id_map, session)
    label = lag.LabelAggregate()
    print(f'DONE LOADING LABELS FROM CSV')

if __name__ == '__main__':


    main()
