from faker import Faker

from runserver import app
from fkcookiecutter.apps.user.models import Role, User, db

fake = Faker()


def create_records():
    with app.app_context():
        for _ in range(1000):
            user = object()

            while user is not None:
                username = fake.name()
                user = User.query.filter_by(username=username).first()

            print(f"username: {username}")
            user_dict = dict(
                username=username, email=fake.email(), active=True,
                first_name=fake.first_name(), last_name=fake.last_name()
            )
            user = User(**user_dict)
            db.session.add(user)
            # db.session.add_all([user1, user2])
            db.session.commit()


def update_record():
    with app.app_context():
        # user = User.query.get(222)    # LegacyAPIWarning
        user = db.session.query(User).get(222)  # LegacyAPIWarning
        print(f"Before: {user}, email: {user.email}")

        user.email = fake.email()
        db.session.add(user)
        db.session.commit()
        print(f"After: {user}, email: {user.email}")

        User.query.filter_by(id=1).update({'email': fake.email()})
        db.session.commit()
        user1 = User.query.get(222)
        print(f"Three: {user1}, email: {user1.email}")


def delete_record():
    with app.app_context():
        user = User.query.order_by(User.id.desc()).first()
        db.session.delete(user)
        db.session.commit()

        # User.query.filter(User.mobile = '12345678910').delete()
        # db.session.commit()


def column_operators():
    col = User.username
    print(dir(col))
    # 倒序：User.username.desc
    # endswith, iendswith, startswith, istartswith, isnot like, ilike, icontains
    # distinct, contains, etc.


def other_query():
    # ORM默认是全表扫描，使⽤load_only函数可以指定字段
    # 查询所有字段
    user = User.query.filter_by(id=1).first()

    # 查询指定字段
    from sqlalchemy.orm import load_only
    User.query.options(load_only(User.name, User.mobile)).filter_by(id=1).first()

    # 聚合查询
    from sqlalchemy import func

    # 关联查询: ForeignKey, primaryjoin,

    # 事务：db.session.rollback()


if __name__ == '__main__':
    column_operators()
