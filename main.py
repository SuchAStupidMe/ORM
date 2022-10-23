# -*- coding: utf-8 -*-

import aiofiles
from fastapi import FastAPI, File, UploadFile
from pydantic import BaseModel
from methods import user_credits_search, engine, plan_insert, plan_performance, y_performance
from tables import tables_check


app = FastAPI()

tables_check()


class User(BaseModel):
    user_id: int


@app.post("/user_credits")
async def user_credits(user: User):
    user = User(user_id=user.user_id)
    return user_credits_search(engine, user.user_id)


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
        return plan_insert(engine)
    except Exception as e:
        return {"Error": "Error processing file", "e": e}


@app.post("/plans_performance")
async def plans_performance(date):
    return plan_performance(date)


@app.post("/year_performance")
async def year_performance(year):
    return y_performance(year)




