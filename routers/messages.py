from fastapi import APIRouter, Form, Depends, HTTPException, status
from db import Database
from dependencies import get_db, validate_user

router = APIRouter(tags=['messages'], prefix="/messages")


@router.post("/{message_id}/upvote")
def upvote_a_message(message_id: int,
                     db: Database = Depends(get_db),
                     user_id: str = Depends(validate_user)):
    stored_message = db.get_one(tablename='messages', cols=[
                                "id", "message", "private", "user_id"], where={"id": message_id})
    if not stored_message:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="message not found")
    if stored_message and stored_message.get('private'):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN,
                            detail="Request not completed")
    if stored_message and stored_message.get('user_id') == user_id:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN,
                            detail="Request not completed")

    db.write('upvotes', cols=["user_id", "message_id"],
             values=[user_id, message_id])
    db.conn.commit()
    return {"status": "upvote recorded"}


@router.get("/")
def get_all_messages_from_guestbook(num: int = 10,
                                    db: Database = Depends(get_db),
                                    user_id: str = Depends(validate_user)):
    messages = db.get(tablename='messages',
                      cols=["id", "message", "private", "created_at"],
                      where={"private": False},
                      or_where={"user_id": user_id, "private": True},
                      limit=num)

    return {"messages": messages}


@router.post('/')
def add_a_new_message_to_guestbook(message: str = Form(...),
                                   private: bool = Form(False),
                                   db: Database = Depends(get_db),
                                   user_id: int = Depends(validate_user)):
    message_id = db.write(tablename='messages',
                          cols=["user_id", "message", "private"],
                          values=[user_id, message, private])
    return {"message_id": message_id}


@router.get("/search")
def search_for_messages_by_keyword(search_term: str,
                                   num: int = 10,
                                   db: Database = Depends(get_db),
                                   user_id: int = Depends(validate_user)):
    public_messages = db.get(tablename="messages",
                             cols=["id", "message", "private"],
                             where={"private": False},
                             contains={"message": search_term},
                             limit=num)
    private_messages = db.get(tablename="messages",
                              cols=["id", "message", "private"],
                              where={"private": True, "user_id": user_id},
                              contains={"message": search_term},
                              limit=num)
    return public_messages + private_messages


@router.get("/popular")
def get_most_popular_messages(db: Database = Depends(get_db)):
    messages = db.get(tablename="popular_messages",
                      cols=["id", "message", "upvotes"])
    db.cursor.fetchall()
    return {"messages": messages}


@router.get("/{message_id}")
def get_existing_messages_from_guestbook(message_id: int,
                                         db: Database = Depends(get_db),
                                         user_id: str = Depends(validate_user)):
    stored_message = db.get_one(tablename='messages',
                                cols=["id", "message", "user_id",
                                      "private", "created_at"],
                                where={"id": message_id})
    if not stored_message:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="message not found")

    if not stored_message.get('user_id') == user_id and stored_message.get('private'):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN,
                            detail="Request not completed")

    return {"id": stored_message.get('id'),
            "message": stored_message.get('message'),
            "created_at": stored_message.get('created_at')}


@router.patch('/{message_id}')
def update_an_existing_message_in_guestbook(message_id: int,
                                            message: str = Form(...),
                                            private: bool = Form(False),
                                            db: Database = Depends(get_db),
                                            user_id: str = Depends(validate_user)):
    original_message = db.get_one(tablename='messages',
                                  cols=["id", "user_id"],
                                  where={"id": message_id})
    if not original_message:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="message not found")
    if original_message.get('user_id') == user_id:
        db.update('messages', ["message", "private"], [
                  message, private], where={"id": message_id})
        db.conn.commit()
        return {"status": "Message updated"}
    raise HTTPException(status_code=status.HTTP_403_FORBIDDEN,
                        detail="Request not completed")


@router.delete("/")
def delete_users_messages_data_from_guestbook(message_id: int,
                                              db: Database = Depends(get_db),
                                              user_id: str = Depends(validate_user)):
    message = db.get_one(tablename="messages", cols=[
                         "id", "user_id", "private"], where={"id": message_id})
    if not message:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="message not found")

    if message.get('user_id') != user_id:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN,
                            detail="Request not completed")

    rows_deleted = db.delete(tablename="messages", where={
                             "user_id": user_id, "id": message_id})
    if rows_deleted == 1:
        return {"status": f"{rows_deleted} row deleted"}
    return {"status": f"{rows_deleted} rows deleted"}
