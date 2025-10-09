print("Hello, World!")  # Hello, World!

# Python keywords:
# False      await      else       import     pass
# None       break      except     in         raise
# True       class      finally    is         return
# and        continue   for        lambda     try
# as         def        from       nonlocal   while
# assert     del        global     not        with
# async      elif       if         or         yield

# Soft keywords:
# match      case       type       _

# Operators:
# +       -       *       **      /       //      %      @
# <<      >>      &       |       ^       ~       :=
# <       >       <=      >=      ==      !=

a, b = 0, 1

while a < 10:
    print(a)  # 0, 1, 1, 2, 3, 5, 8
    a, b = b, a + b

# example loop with try/except/else
tries, v = 0, None
while tries < 3:
    try:
        v = int(input("Please enter a number: "))
        break
    except ValueError:
        tries += 1
        print("Invalid input")
else:
    print(f"Gave up after {tries} tries")

print(f"You entered: {v}")  # You entered: <number> | None

if v is None:
    print("No number entered")
elif v < 0:
    print("Negative number")
elif v == 0:
    print("Zero")
else:
    print("Positive number")

# arrays and collections can span lines
numbers = [1, 2, 3, 4, 5]

for n in numbers:
    print(n)  # 1, 2, 3, 4, 5

items = {"apple": 1, "banana": 2, "cherry": 3}

for item, count in items.items():
    print(f"{item}: {count}")  # apple: 1, banana: 2, cherry: 3

for item in items:
    print(f"{item}: {items[item]}")  # apple: 1, banana: 2, cherry: 3

squared = [n**2 for n in numbers]
print(squared)  # [1, 4, 9, 16, 25]
print(sum(numbers))  # 15
print(len(numbers))  # 5
print(max(numbers))  # 5
print(min(numbers))  # 1

print(list(range(5)))  # 0, 1, 2, 3, 4
print(list(range(5, 10)))  # 5, 6, 7, 8, 9
print(list(range(0, 10, 2)))  # 0, 2, 4, 6, 8

# array unpacking
(x, y, z) = numbers[2:]
print(x, y, z)  # 3 4 5

# example else with break; the else fires if the break does not
for n in range(2, 10):
    for x in range(2, n):
        if n % x == 0:
            print(n, "equals", x, "*", n // x)  # # floor division
            break
    else:
        print(
            n, "is a prime number"
        )  # 2 is a prime number, 3 is a prime number, 5 is a prime number, 7 is a prime number


# empty Classes
class SomeClass:
    pass


class AnotherClass: ...


print(SomeClass)  # <class '__main__.SomeClass'>
print(AnotherClass)  # <class '__main__.AnotherClass'>


# function
def http_error(status):
    match status:
        case 400:
            return "Bad request"
        case 401 | 403:  # multiple matches
            return "Not allowed"
        case 404:
            return "Not found"
        case _ if status >= 500:  # guard
            return "Server error"
        case _:  # default
            return "Unknown error"


for status in [400, 401, 403, 404, 500, 502, 200]:
    print(f"{status}: {http_error(status=status)}")
    # 400: Bad request
    # 401: Not allowed
    # 403: Not allowed
    # 404: Not found
    # 500: Server error
    # 502: Server error
    # 200: Unknown error


# variadic function
def multiply(*numbers):
    if len(numbers) == 0:
        return None
    result = 1
    for n in numbers:
        result *= n
    return result


print(multiply())  # None
print(multiply(2, 3))  # 6
print(multiply(2, 3, 4))  # 24


# variadic keyword arguments
def concatenate(*, separator=", ", **arguments):
    if len(arguments) == 0:
        return None
    result = ""
    for key, value in arguments.items():
        result += f"{key}={value}{separator}"
    return result[: -len(separator)]  # remove last separator


print(concatenate())  # None
print(concatenate(separator="; ", a=1, b=2, c=3))  # a=1; b=2; c=3
print(concatenate(name="Alice", age=30))  # name=Alice, age=30

foo = "bar"
print(f"{foo}")  # bar (only the value)
print(f"{foo = }")  # foo = 'bar' (expression text is included)

print(f"{foo!s}")  # bar (only the value)
print(f"{foo!r}")  # 'bar' (string representation)
print(f"{foo!a}")  # 'bar' (ascii representation)


# import asyncio
# from operator import itemgetter
# import sys
# from os import environ
# from typing import Any, AsyncGenerator
# from google.genai import types as GenaiTypes
# from google.genai.client import AsyncClient as AGenaiClient, Client as GenaiClient
# from psycopg.conninfo import make_conninfo
# from psycopg.rows import DictRow, dict_row
# import psycopg
# from psycopg import AsyncConnection, sql
# from psycopg_pool import AsyncConnectionPool

#        self.genai_client = GenaiClient(api_key=environ.get("GOOGLE_API_KEY")).aio#


#         async def create_user(client: AGenaiClient, **values: Any) -> dict[str, Any]:
#             text: str = values["biography"]
#             embedding: GenaiTypes.EmbedContentResponse = (
#                 await client.models.embed_content(  # type: ignore
#                     model="gemini-embedding-001",
#                     contents=[text],
#                     config=GenaiTypes.EmbedContentConfig(
#                         task_type="RETRIEVAL_DOCUMENT",
#                         output_dimensionality=1536,
#                     ),
#                 )
#             )
#             return values | {"embedding": embedding.embeddings[0].values}  # type: ignore
