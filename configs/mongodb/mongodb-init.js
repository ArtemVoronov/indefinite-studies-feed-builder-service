print('#################################################################');
db.createUser(
    {
        user: "mongo_admin",
        pwd: "mongo_admin_password",
        roles: [
            {
                role: "readWrite",
                db: "feeddb"
            }
        ]
    }
);
print('#################################################################');