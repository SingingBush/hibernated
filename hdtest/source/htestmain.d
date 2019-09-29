module htestmain;

private import ddbc.drivers.sqliteddbc;
private import ddbc.drivers.mysqlddbc;
private import ddbc.drivers.pgsqlddbc;

private import std.stdio;
private import std.string;
private import std.conv;
private import hibernated.core;
private import std.traits;

// Annotations of entity classes
@Table( "gebruiker" )
class Person {
    long id;
    string name;
    int some_field_with_underscore;
    @ManyToMany // cannot be inferred, requires annotation
    LazyCollection!PersonRole roles;
    //@ManyToOne
    MyGroup group;

    @OneToMany
    Address[] addresses;

    Asset[] assets;

    override string toString() {
        return format("{id:%s, name:%s, roles:%s, group:%s}", id, name, roles, group);
    }
}

class PersonRole {
    int id;
    string name;
    @ManyToMany // w/o this annotation will be OneToMany by convention
    LazyCollection!Person users;

    override string toString() {
        return format("{id:%s, name:%s}", id, name);
    }
}

class Address {
    @Generated @Id int id;
    Person person;
    string street;
    string town;
    string country;

    override string toString() {
        return format("{id:%s, person:%s, street:%s, town:%s, country:%s}", id, person, street, town, country);
    }
}

class Asset {
    @Generated @Id int id;
    Person person;
    string name;
}

@Entity
class MyGroup {
    long id;
    string name;
    @OneToMany
    LazyCollection!Person users;

    override string toString() {
        return format("{id:%s, name:%s}", id, name);
    }
}

void testHibernate(const string uri) {
    
    Driver driver;
    Dialect dialect;

    if (uri.startsWith("sqlite")) {
        driver = new SQLITEDriver();
        dialect = new SQLiteDialect();
    } else if (uri.startsWith("mysql")) {
        driver = new MySQLDriver();
        dialect = new MySQLDialect();
    } else if (uri.startsWith("postgresql")) {
        driver = new PGSQLDriver();
        dialect = new PGSQLDialect();
    } else if (uri.startsWith("odbc")) {
        stderr.writeln("No dialect for ODBC has been written yet");
        //driver = new ODBCDriver();
        //dialect = new MSSQLDialect();
    } else {
        stderr.writeln("No valid driver type specified");
    }

    DataSource ds = new ConnectionPoolDataSourceImpl(driver, uri);

    // setup DB connection
    // version( USE_SQLITE )
    // {
    //     import ddbc.drivers.sqliteddbc;
    //     string[string] params;
    //     DataSource ds = new ConnectionPoolDataSourceImpl(new SQLITEDriver(), "zzz.db", params);
    //     Dialect dialect = new SQLiteDialect();
    // }
    // else version( USE_PGSQL )
    // {
    //     import ddbc.drivers.pgsqlddbc;
    //     string url = PGSQLDriver.generateUrl( "/tmp", 5432, "testdb" );
    //     string[string] params;
    //     params["user"] = "hdtest";
    //     params["password"] = "secret";
    //     params["ssl"] = "true";

    //     DataSource ds = new ConnectionPoolDataSourceImpl(new PGSQLDriver(), url, params);
    //     Dialect dialect = new PGSQLDialect();
    // }

    // create metadata from annotations
    writeln("Creating schema from class list");
    EntityMetaData schema = new SchemaInfoImpl!(Person, PersonRole, Address, Asset, MyGroup);
    //writeln("Creating schema from module list");
    //EntityMetaData schema2 = new SchemaInfoImpl!(htestmain);


    writeln("Creating session factory");
    // create session factory
    SessionFactory factory = new SessionFactoryImpl(schema, dialect, ds);
    scope(exit) factory.close();

    writeln("Creating DB schema");
    DBInfo db = factory.getDBMetaData();
    {
        Connection conn = ds.getConnection();
        scope(exit) conn.close();
        db.updateDBSchema(conn, true, true);
    }


    // create session
    Session sess = factory.openSession();
    scope(exit) sess.close();

    // use session to access DB

    writeln("Querying empty DB");
    Query q = sess.createQuery("FROM Person ORDER BY name");
    Person[] list = q.list!Person();
    writeln("Result size is " ~ to!string(list.length));

    // create sample data
    writeln("Creating sample schema");
    MyGroup grp1 = new MyGroup();
    grp1.name = "Group-1";
    MyGroup grp2 = new MyGroup();
    grp2.name = "Group-2";
    MyGroup grp3 = new MyGroup();
    grp3.name = "Group-3";
    //
    PersonRole r10 = new PersonRole();
    r10.name = "role10";
    PersonRole r11 = new PersonRole();
    r11.name = "role11";

    // create a Person called Alex with an address and an asset
    Person u10 = new Person();
    u10.name = "Alex";
    u10.roles = [r10, r11];
    u10.group = grp3;
    auto address = new Address();
    address.street = "Some Street";
    address.town = "Big Town";
    address.country = "Alaska";
    address.person = u10;

    writefln("Saving Address: %s", address);
    sess.save(address);

    u10.addresses = [address];
    auto asset = new Asset();
    asset.name = "Something Precious";
    asset.person = u10;
    writefln("Saving Asset: %s", asset);
    sess.save(asset);
    u10.assets = [asset];

    Person u12 = new Person();
    u12.name = "Arjan";
    u12.roles = [r10, r11];
    u12.group = grp2;

    Person u13 = new Person();
    u13.name = "Wessel";
    u13.roles = [r10, r11];
    u13.group = grp2;

    writeln("saving group 1-2-3" );
    sess.save( grp1 );
    sess.save( grp2 );
    sess.save( grp3 );

    writeln("Saving PersonRole r10: " ~ r10.name);
    sess.save(r10);

    writeln("Saving PersonRole r11: " ~ r11.name);
    sess.save(r11);

    writeln("Saving Person u10: " ~ u10.name);
    sess.save(u10);

    writeln("Saving Person u12: " ~ u12.name);
    sess.save(u12);

    writeln("Saving Person u13: " ~ u13.name);
    sess.save(u13);

    writeln("Loading Person");
    // load and check data
    auto qresult = sess.createQuery("FROM Person WHERE name=:Name and some_field_with_underscore != 42").setParameter("Name", "Alex");
    
    assert((qresult.list!Person()).length == 1, "There should be 1 row which has a Person with name of 'Alex'");

    // Variant[][] rows = qresult.listRows();
    writefln( "query result: %s", qresult.listRows() );

    Person u11 = qresult.uniqueResult!Person();
    //Person u11 = sess.createQuery("FROM Person WHERE name=:Name and some_field_with_underscore != 42").setParameter("Name", "Alex").uniqueResult!Person();
    writefln("Checking Person 11 : %s", u11);
    assert(u11.name == "Alex");
    assert(u11.roles.length == 2);
    assert(u11.roles[0].name == "role10" || u11.roles.get()[0].name == "role11");
    assert(u11.roles[1].name == "role10" || u11.roles.get()[1].name == "role11");
    assert(u11.roles[0].users.length == 3);
    assert(u11.roles[0].users[0] == u10);

    assert(u11.addresses.length == 1);
    assert(u11.addresses[0].street == "Some Street");
    assert(u11.addresses[0].town == "Big Town");
    assert(u11.addresses[0].country == "Alaska");

    assert(u11.assets.length == 1);
    assert(u11.assets[0].name == "Something Precious");

    // selecting all from address table should return a row that joins to the person table
    auto allAddresses = sess.createQuery("FROM Address").list!Address();
    assert(allAddresses.length == 1);
    writefln("Found address : %s", allAddresses[0]);
    assert(allAddresses[0].street == "Some Street");
    assert(allAddresses[0].person == u11);

    // selecting all from asset table should return a row that joins to the person table
    auto allAssets = sess.createQuery("FROM Asset").list!Asset();
    assert(allAssets.length == 1);
    writefln("Found asset : %s", allAssets[0]);
    assert(allAssets[0].name == "Something Precious");
    assert(allAssets[0].person == u11);

    // now test something else
    writeln("Test retrieving Persons by group... (ManyToOne relationship)");
    auto qPersonsByGroup = sess.createQuery("FROM Person WHERE group=:group_id").setParameter("group_id", grp2.id);
    Person[] usersByGroup = qPersonsByGroup.list!Person();
    assert(usersByGroup.length == 2); // Person 2 and Person 2

    //writeln("Removing Person");
    // remove reference
    //std.algorithm.remove(u11.roles.get(), 0);
    //sess.update(u11);

    // remove entity
    //sess.remove(u11);
}

int main(string[] args)
{
    static if(__traits(compiles, (){ import std.experimental.logger; } )) {
		import std.experimental.logger;
		globalLogLevel(LogLevel.all);
	}

    if (args.length < 2) {
        writefln("A db URI arg is required!");
        return 1;
    }

    string uri = args[1];

    stderr.writefln("Testing HibernateD with ddbc uri:\t%s", uri);

    if (uri.startsWith("ddbc:")) {
		uri = uri[5 .. $]; // strip out ddbc: prefix
	}

    testHibernate(uri);
    return 0;
}
