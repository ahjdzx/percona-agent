/*
   Copyright (c) 2014, Percona LLC and/or its affiliates. All rights reserved.

   This program is free software: you can redistribute it and/or modify
   it under the terms of the GNU Affero General Public License as published by
   the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU Affero General Public License for more details.

   You should have received a copy of the GNU Affero General Public License
   along with this program.  If not, see <http://www.gnu.org/licenses/>
*/

package installer_test

import (
	i "github.com/percona/percona-agent/bin/percona-agent-installer/installer"
	"github.com/percona/percona-agent/mysql"
	"github.com/percona/percona-agent/test"
	. "gopkg.in/check.v1"
	"io/ioutil"
)

type MySQLTestSuite struct {
}

var _ = Suite(&MySQLTestSuite{})

var sample = test.RootDir + "/mysql/"

// --------------------------------------------------------------------------

func (s *MySQLTestSuite) TestMakeGrant(t *C) {
	user := "new-user"
	pass := "some pass"
	dsn := mysql.DSN{
		Username: "user",
		Password: "pass",
	}

	dsn.Hostname = "localhost"
	maxOpenConnections := int64(1)
	got := i.MakeGrant(dsn, user, pass, maxOpenConnections)
	expect := []string{
		"GRANT SUPER, PROCESS, USAGE, SELECT ON *.* TO 'new-user'@'localhost' IDENTIFIED BY 'some pass' WITH MAX_USER_CONNECTIONS 1",
		"GRANT UPDATE, DELETE, DROP ON performance_schema.* TO 'new-user'@'localhost' IDENTIFIED BY 'some pass' WITH MAX_USER_CONNECTIONS 1",
	}
	t.Check(got, DeepEquals, expect)

	dsn.Hostname = "127.0.0.1"
	got = i.MakeGrant(dsn, user, pass, maxOpenConnections)
	expect = []string{
		"GRANT SUPER, PROCESS, USAGE, SELECT ON *.* TO 'new-user'@'127.0.0.1' IDENTIFIED BY 'some pass' WITH MAX_USER_CONNECTIONS 1",
		"GRANT UPDATE, DELETE, DROP ON performance_schema.* TO 'new-user'@'127.0.0.1' IDENTIFIED BY 'some pass' WITH MAX_USER_CONNECTIONS 1",
	}
	t.Check(got, DeepEquals, expect)

	dsn.Hostname = "10.1.1.1"
	got = i.MakeGrant(dsn, user, pass, maxOpenConnections)
	expect = []string{
		"GRANT SUPER, PROCESS, USAGE, SELECT ON *.* TO 'new-user'@'%' IDENTIFIED BY 'some pass' WITH MAX_USER_CONNECTIONS 1",
		"GRANT UPDATE, DELETE, DROP ON performance_schema.* TO 'new-user'@'%' IDENTIFIED BY 'some pass' WITH MAX_USER_CONNECTIONS 1",
	}
	t.Check(got, DeepEquals, expect)

	dsn.Hostname = ""
	dsn.Socket = "/var/lib/mysql.sock"
	got = i.MakeGrant(dsn, user, pass, maxOpenConnections)
	expect = []string{
		"GRANT SUPER, PROCESS, USAGE, SELECT ON *.* TO 'new-user'@'localhost' IDENTIFIED BY 'some pass' WITH MAX_USER_CONNECTIONS 1",
		"GRANT UPDATE, DELETE, DROP ON performance_schema.* TO 'new-user'@'localhost' IDENTIFIED BY 'some pass' WITH MAX_USER_CONNECTIONS 1",
	}
	t.Check(got, DeepEquals, expect)
}

func (s *MySQLTestSuite) TestParseMySQLDefaults(t *C) {
	output, err := ioutil.ReadFile(sample + "/defaults001")
	t.Assert(err, IsNil)
	got := i.ParseMySQLDefaults(string(output))
	expect := &mysql.DSN{
		Hostname: "localhost",
		Username: "root",
	}
	t.Check(got, DeepEquals, expect)
}
