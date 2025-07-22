from sqlalchemy.orm import Session, aliased
from sqlalchemy import and_, or_
from typing import *
from fastapi import Request, UploadFile, HTTPException
import models, schemas
import boto3
import jwt
import datetime
import requests
import math
import random
import asyncio
from pathlib import Path


async def get_join3(db: Session):

    try:
        is_not_null_condition = "IS NOT NULL"
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, str(e))

    us = aliased(models.Users)
    query = db.query(models.Posts, us)

    query = query.join(us, and_(models.Posts.title != is_not_null_condition))

    test = query.first()

    if test:
        s1, s2 = test
        test = {
            "test_1": s1.to_dict() if hasattr(s1, "to_dict") else vars(s1),
            "test_2": s2.to_dict() if hasattr(s2, "to_dict") else vars(s2),
        }

    res = {
        "test": test,
    }
    return res


async def get_users(db: Session):

    query = db.query(models.Users)

    users_all = query.all()
    users_all = (
        [new_data.to_dict() for new_data in users_all] if users_all else users_all
    )
    res = {
        "users_all": users_all,
    }
    return res


async def get_users_user_id(db: Session, user_id: int):

    query = db.query(models.Users)
    query = query.filter(and_(models.Users.user_id == user_id))

    users_one = query.first()

    users_one = (
        (users_one.to_dict() if hasattr(users_one, "to_dict") else vars(users_one))
        if users_one
        else users_one
    )

    res = {
        "users_one": users_one,
    }
    return res


async def put_users_user_id(db: Session, raw_data: schemas.PutUsersUserId):
    user_id: int = raw_data.user_id
    username: str = raw_data.username
    email: str = raw_data.email

    query = db.query(models.Users)
    query = query.filter(and_(models.Users.user_id == user_id))
    users_edited_record = query.first()

    if users_edited_record:
        for key, value in {
            "email": email,
            "user_id": user_id,
            "username": username,
        }.items():
            setattr(users_edited_record, key, value)

        db.commit()
        db.refresh(users_edited_record)

        users_edited_record = (
            users_edited_record.to_dict()
            if hasattr(users_edited_record, "to_dict")
            else vars(users_edited_record)
        )
    res = {
        "users_edited_record": users_edited_record,
    }
    return res


async def delete_users_user_id(db: Session, raw_data: schemas.DeleteUsersUserId):
    user_id: int = raw_data.user_id

    query = db.query(models.Users)
    query = query.filter(and_(models.Users.user_id == user_id))

    record_to_delete = query.first()
    if record_to_delete:
        db.delete(record_to_delete)
        db.commit()
        users_deleted = record_to_delete.to_dict()
    else:
        users_deleted = record_to_delete
    res = {
        "users_deleted": users_deleted,
    }
    return res


async def get_posts(db: Session):

    query = db.query(models.Posts)

    posts_all = query.all()
    posts_all = (
        [new_data.to_dict() for new_data in posts_all] if posts_all else posts_all
    )
    res = {
        "posts_all": posts_all,
    }
    return res


async def get_posts_post_id(db: Session, post_id: int):

    query = db.query(models.Posts)
    query = query.filter(and_(models.Posts.post_id == post_id))

    posts_one = query.first()

    posts_one = (
        (posts_one.to_dict() if hasattr(posts_one, "to_dict") else vars(posts_one))
        if posts_one
        else posts_one
    )

    res = {
        "posts_one": posts_one,
    }
    return res


async def put_posts_post_id(db: Session, raw_data: schemas.PutPostsPostId):
    post_id: int = raw_data.post_id
    user_id: int = raw_data.user_id
    title: str = raw_data.title
    content: str = raw_data.content
    published_at: datetime.datetime = raw_data.published_at

    query = db.query(models.Posts)
    query = query.filter(and_(models.Posts.post_id == post_id))
    posts_edited_record = query.first()

    if posts_edited_record:
        for key, value in {
            "title": title,
            "content": content,
            "post_id": post_id,
            "user_id": user_id,
            "published_at": published_at,
        }.items():
            setattr(posts_edited_record, key, value)

        db.commit()
        db.refresh(posts_edited_record)

        posts_edited_record = (
            posts_edited_record.to_dict()
            if hasattr(posts_edited_record, "to_dict")
            else vars(posts_edited_record)
        )
    res = {
        "posts_edited_record": posts_edited_record,
    }
    return res


async def delete_posts_post_id(db: Session, raw_data: schemas.DeletePostsPostId):
    post_id: int = raw_data.post_id

    query = db.query(models.Posts)
    query = query.filter(and_(models.Posts.post_id == post_id))

    record_to_delete = query.first()
    if record_to_delete:
        db.delete(record_to_delete)
        db.commit()
        posts_deleted = record_to_delete.to_dict()
    else:
        posts_deleted = record_to_delete
    res = {
        "posts_deleted": posts_deleted,
    }
    return res


async def get_comments(db: Session):

    query = db.query(models.Comments)

    comments_all = query.all()
    comments_all = (
        [new_data.to_dict() for new_data in comments_all]
        if comments_all
        else comments_all
    )
    res = {
        "comments_all": comments_all,
    }
    return res


async def get_comments_comment_id(db: Session, comment_id: int):

    query = db.query(models.Comments)
    query = query.filter(and_(models.Comments.comment_id == comment_id))

    comments_one = query.first()

    comments_one = (
        (
            comments_one.to_dict()
            if hasattr(comments_one, "to_dict")
            else vars(comments_one)
        )
        if comments_one
        else comments_one
    )

    res = {
        "comments_one": comments_one,
    }
    return res


async def post_comments(db: Session, raw_data: schemas.PostComments):
    comment_id: int = raw_data.comment_id
    post_id: int = raw_data.post_id
    user_id: int = raw_data.user_id
    comment_text: str = raw_data.comment_text
    commented_at: datetime.datetime = raw_data.commented_at

    record_to_be_added = {
        "post_id": post_id,
        "user_id": user_id,
        "comment_id": comment_id,
        "comment_text": comment_text,
        "commented_at": commented_at,
    }
    new_comments = models.Comments(**record_to_be_added)
    db.add(new_comments)
    db.commit()
    db.refresh(new_comments)
    comments_inserted_record = new_comments.to_dict()

    res = {
        "comments_inserted_record": comments_inserted_record,
    }
    return res


async def put_comments_comment_id(db: Session, raw_data: schemas.PutCommentsCommentId):
    comment_id: int = raw_data.comment_id
    post_id: int = raw_data.post_id
    user_id: int = raw_data.user_id
    comment_text: str = raw_data.comment_text
    commented_at: datetime.datetime = raw_data.commented_at

    query = db.query(models.Comments)
    query = query.filter(and_(models.Comments.comment_id == comment_id))
    comments_edited_record = query.first()

    if comments_edited_record:
        for key, value in {
            "post_id": post_id,
            "user_id": user_id,
            "comment_id": comment_id,
            "comment_text": comment_text,
            "commented_at": commented_at,
        }.items():
            setattr(comments_edited_record, key, value)

        db.commit()
        db.refresh(comments_edited_record)

        comments_edited_record = (
            comments_edited_record.to_dict()
            if hasattr(comments_edited_record, "to_dict")
            else vars(comments_edited_record)
        )
    res = {
        "comments_edited_record": comments_edited_record,
    }
    return res


async def delete_comments_comment_id(
    db: Session, raw_data: schemas.DeleteCommentsCommentId
):
    comment_id: int = raw_data.comment_id

    query = db.query(models.Comments)
    query = query.filter(and_(models.Comments.comment_id == comment_id))

    record_to_delete = query.first()
    if record_to_delete:
        db.delete(record_to_delete)
        db.commit()
        comments_deleted = record_to_delete.to_dict()
    else:
        comments_deleted = record_to_delete
    res = {
        "comments_deleted": comments_deleted,
    }
    return res


async def get_join1(db: Session):

    try:
        userrecords = "alice"
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, str(e))

    us = aliased(models.Users)
    query = db.query(models.Users, us)

    query = query.join(us, and_(models.Users.username == userrecords))

    userrecords1 = query.first()

    if userrecords1:
        s1, s2 = userrecords1
        userrecords1 = {
            "userrecords1_1": s1.to_dict() if hasattr(s1, "to_dict") else vars(s1),
            "userrecords1_2": s2.to_dict() if hasattr(s2, "to_dict") else vars(s2),
        }

    res = {
        "userrecords1": userrecords1,
    }
    return res


async def post_users(db: Session, raw_data: schemas.PostUsers):
    user_id: int = raw_data.user_id
    username: str = raw_data.username
    email: str = raw_data.email

    record_to_be_added = {"email": email, "user_id": user_id, "username": username}
    new_users = models.Users(**record_to_be_added)
    db.add(new_users)
    db.commit()
    db.refresh(new_users)
    users_inserted_record = new_users.to_dict()

    user_list: str = username

    for user_loop in range(1, 1):

        if user_id == user_id:
            pass

    res = {
        "users_inserted_record": users_inserted_record,
        "hfg": user_loop,
    }
    return res


async def get_join4(db: Session):

    from datetime import datetime

    try:
        published_after = datetime(2025, 7, 1)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, str(e))

    us = aliased(models.Users)
    query = db.query(models.Posts, us)

    query = query.join(us, and_(models.Posts.published_at == published_after))

    test = query.first()

    if test:
        s1, s2 = test
        test = {
            "test_1": s1.to_dict() if hasattr(s1, "to_dict") else vars(s1),
            "test_2": s2.to_dict() if hasattr(s2, "to_dict") else vars(s2),
        }

    res = {
        "test": test,
    }
    return res


async def get_join2(db: Session):

    try:
        email = "bob@example.com"
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, str(e))

    us = aliased(models.Comments)
    query = db.query(models.Users, us)

    query = query.join(us, and_(models.Users.email == email))

    test_email = query.first()

    if test_email:
        s1, s2 = test_email
        test_email = {
            "test_email_1": s1.to_dict() if hasattr(s1, "to_dict") else vars(s1),
            "test_email_2": s2.to_dict() if hasattr(s2, "to_dict") else vars(s2),
        }

    res = {
        "test_email": test_email,
    }
    return res


async def post_posts(db: Session, raw_data: schemas.PostPosts):
    post_id: int = raw_data.post_id
    user_id: int = raw_data.user_id
    title: str = raw_data.title
    content: str = raw_data.content
    published_at: datetime.datetime = raw_data.published_at

    record_to_be_added = {
        "title": title,
        "content": content,
        "post_id": post_id,
        "user_id": user_id,
        "published_at": published_at,
    }
    new_posts = models.Posts(**record_to_be_added)
    db.add(new_posts)
    db.commit()
    db.refresh(new_posts)
    posts_inserted_record = new_posts.to_dict()

    user_list: str = title

    for user_loop in range(post_id, user_id):
        pass

    res = {
        "posts_inserted_record": posts_inserted_record,
        "mnbb": user_list,
    }
    return res
