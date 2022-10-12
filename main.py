# -*- coding: utf-8 -*-

import aiofiles
from fastapi import FastAPI, File, UploadFile
from pydantic import BaseModel
from mysql import user_credits_search, file_db, initial_check, plan_insert


TABLES = ['users', 'payments', 'plans', 'credits', 'dictionary']
initial_check(TABLES)

app = FastAPI()


class User(BaseModel):
    user_id: int


@app.post("/user_credits")
async def user_credits(user: User):
    user = User(user_id=user.user_id)
    return user_credits_search(file_db, user.user_id)


@app.post("/plans_insert")
async def plans_insert(file: UploadFile = File()):
    try:
        try:
            async with aiofiles.open('plan.xlsx', 'wb') as out_file:
                content = await file.read()
                await out_file.write(content)

        except Exception as e:
            return {"Error": "Error uploading file", "e": e}
        finally:
            file.file.close()
        return plan_insert(file_db)
    except Exception as e:
        return {"Error": "Error processing file", "e": e}





