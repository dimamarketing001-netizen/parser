"""Папки-подборки из темы таксономии (23.11.2026)."""
import os, sys, tempfile
from pathlib import Path
os.environ.setdefault("PARSYA_SECRET_KEY", "test-secret-key-that-is-long-enough-00000000")
os.environ.setdefault("PARSYA_WORKER_TOKEN", "test-worker-token-that-is-long-enough-00000000")
os.environ.setdefault("PARSYA_COOKIE_SECURE", "0")
os.environ.setdefault("PARSYA_AI_INTENT_ENABLED", "0")
os.environ.setdefault("PARSYA_SQLITE_PATH", str(Path(tempfile.gettempdir()) / f"parsya-test-topic-{os.getpid()}.sqlite3"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from contextlib import closing
import pytest
from fastapi import HTTPException
import app.main as m

PR = {"kind": "user", "user_id": 77, "role": "superuser", "email": "f@t"}


def _seed():
    with closing(m.connect()) as db:
        db.execute("INSERT OR IGNORE INTO users(id,email,password_hash,role,created_at) VALUES(77,'f@t','x','superuser','2026-01-01')")
        db.execute("INSERT OR IGNORE INTO jobs(id,status,city,key_text,created_at,owner_user_id) VALUES(7700,'succeeded','Москва','юрист','2026-11-23T00:00:00+00:00',77)")
        rows = [(7701, "fp-a", "Юрист по разводу", "Юристы", "Семейные споры"),
                (7702, "fp-a", "Юрист по разводу (копия)", "Юристы", "Семейные споры"),   # то же объявление — одна карточка
                (7703, "fp-b", "Взыщем долг", "Юристы", "Взыскание долгов"),
                (7704, "fp-c", "Химчистка пальто", "Хозяйство и уборка", "Химчистка одежды")]
        for cid, fp, title, mn, sb in rows:
            db.execute("INSERT OR IGNORE INTO creatives(id,creative_hash,content_fingerprint,platform,domain,title,captured_at,ai_intent,ai_intent_main,ai_intent_sub) "
                       "VALUES(?,?,?,?,?,?,?,?,?,?)", (cid, f"h{cid}", fp, "dzen", "x.ru", title, "2026-11-23T00:00:00+00:00", "x", mn, sb))
            db.execute("INSERT INTO occurrences(job_id,creative_id,platform,observed_at) VALUES(7700,?,?,?)", (cid, "dzen", "2026-11-23T00:00:01+00:00"))
        db.commit()


def test_main_topic_makes_folder_with_subfolders_and_is_idempotent():
    _seed()
    r = m.api_collection_from_topic(m.TopicFolderIn(path="Юристы", filters={"job_id": 7700}), principal=PR)
    assert r["found"] == 2 and r["added"] == 2 and r["folders_created"] == 3
    assert {f["name"]: f["items"] for f in r["folders"]} == {"Семейные споры": 1, "Взыскание долгов": 1}
    lst = {c["name"]: c for c in m.api_list_collections(principal=PR)["items"]}
    root = lst["Юристы"]
    assert root["parent_id"] is None and root["children_count"] == 2 and root["items_count"] == 2
    assert lst["Семейные споры"]["parent_id"] == root["id"] and lst["Семейные споры"]["topic_main"] == "Юристы"
    # повтор: дублей нет, ничего нового не добавлено
    again = m.api_collection_from_topic(m.TopicFolderIn(path="Юристы", filters={"job_id": 7700}), principal=PR)
    assert again["folders_created"] == 0 and again["added"] == 0 and again["root_id"] == r["root_id"]


def test_sub_topic_goes_into_main_folder_and_delete_cascades():
    _seed()
    r = m.api_collection_from_topic(m.TopicFolderIn(path="Хозяйство и уборка>Химчистка одежды", filters={"job_id": 7700}), principal=PR)
    assert r["added"] == 1 and [f["name"] for f in r["folders"]] == ["Химчистка одежды"]
    lst = {c["name"]: c for c in m.api_list_collections(principal=PR)["items"]}
    assert lst["Химчистка одежды"]["parent_id"] == lst["Хозяйство и уборка"]["id"] == r["root_id"]
    assert r["focus_id"] == lst["Химчистка одежды"]["id"]
    m.api_delete_collection(r["root_id"], principal=PR)
    names = {c["name"] for c in m.api_list_collections(principal=PR)["items"]}
    assert "Хозяйство и уборка" not in names and "Химчистка одежды" not in names


def test_bad_or_empty_topic():
    _seed()
    with pytest.raises(HTTPException) as e:
        m.api_collection_from_topic(m.TopicFolderIn(path="Нет такого"), principal=PR)
    assert e.value.status_code == 422
    with pytest.raises(HTTPException) as e:
        m.api_collection_from_topic(m.TopicFolderIn(path="Авто>Шины и диски", filters={"job_id": 7700}), principal=PR)
    assert e.value.status_code == 404


def test_deep_items_include_subfolders():
    _seed()
    r = m.api_collection_from_topic(m.TopicFolderIn(path="Юристы", filters={"job_id": 7700}), principal=PR)
    assert m.api_list_collection_items(r["root_id"], principal=PR)["items"] == []
    deep = m.api_list_collection_items(r["root_id"], deep=1, principal=PR)["items"]
    assert sorted(x["group_key"] for x in deep) == ["fp-a", "fp-b"]
    assert {x["folder_id"] for x in deep} == {f["id"] for f in r["folders"]}


def test_manual_subfolder_create_and_move():
    _seed()
    root = m.api_create_collection(m.CollectionCreateIn(name="Клиенты"), principal=PR)["id"]
    sub = m.api_create_collection(m.CollectionCreateIn(name="Иванов", parent_id=root), principal=PR)["id"]
    lst = {c["id"]: c for c in m.api_list_collections(principal=PR)["items"]}
    assert lst[sub]["parent_id"] == root and lst[root]["children_count"] == 1
    with pytest.raises(HTTPException):  # только два уровня
        m.api_create_collection(m.CollectionCreateIn(name="Глубже", parent_id=sub), principal=PR)
    other = m.api_create_collection(m.CollectionCreateIn(name="Разное"), principal=PR)["id"]
    m.api_move_collection(other, m.CollectionMoveIn(parent_id=root), principal=PR)
    with pytest.raises(HTTPException):  # папку с подпапками в другую папку нельзя
        m.api_move_collection(root, m.CollectionMoveIn(parent_id=other), principal=PR)
    with pytest.raises(HTTPException):
        m.api_move_collection(root, m.CollectionMoveIn(parent_id=root), principal=PR)
    m.api_move_collection(other, m.CollectionMoveIn(parent_id=None), principal=PR)
    lst = {c["id"]: c for c in m.api_list_collections(principal=PR)["items"]}
    assert lst[other]["parent_id"] is None and lst[root]["children_count"] == 1


def test_main_topic_with_picked_subs_only_makes_those_subfolders():
    _seed()
    with closing(m.connect()) as db:  # чистый лист для этого пользователя
        db.execute("DELETE FROM collection_items WHERE collection_id IN (SELECT id FROM collections WHERE owner_user_id=77)")
        db.execute("DELETE FROM collections WHERE owner_user_id=77"); db.commit()
    r = m.api_collection_from_topic(m.TopicFolderIn(path="Юристы", filters={"job_id": 7700}, subs=["Семейные споры"]), principal=PR)
    assert [f["name"] for f in r["folders"]] == ["Семейные споры"] and r["found"] == 1
    names = {c["name"] for c in m.api_list_collections(principal=PR)["items"]}
    assert "Взыскание долгов" not in names and {"Юристы", "Семейные споры"} <= names
    with pytest.raises(HTTPException):
        m.api_collection_from_topic(m.TopicFolderIn(path="Юристы", subs=["Нет такой"]), principal=PR)
