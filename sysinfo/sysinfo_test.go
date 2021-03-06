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

package sysinfo_test

import (
	"fmt"
	"github.com/percona/cloud-protocol/proto"
	"github.com/percona/percona-agent/pct"
	"github.com/percona/percona-agent/sysinfo"
	"github.com/percona/percona-agent/test/mock"
	. "gopkg.in/check.v1"
	"testing"
)

// Hook up gocheck into the "go test" runner.
func Test(t *testing.T) { TestingT(t) }

/////////////////////////////////////////////////////////////////////////////
// Manager test suite
/////////////////////////////////////////////////////////////////////////////

type ManagerTestSuite struct {
	logChan chan *proto.LogEntry
	logger  *pct.Logger
}

var _ = Suite(&ManagerTestSuite{})

func (s *ManagerTestSuite) SetUpSuite(t *C) {
	s.logChan = make(chan *proto.LogEntry, 10)
	s.logger = pct.NewLogger(s.logChan, sysinfo.SERVICE_NAME+"-manager-test")
}

// --------------------------------------------------------------------------

func (s *ManagerTestSuite) TestStartStopHandleManager(t *C) {
	var err error

	// Create service
	sysinfoService := mock.NewSysinfoService()

	// Create manager
	m := sysinfo.NewManager(s.logger)
	t.Assert(m, Not(IsNil), Commentf("Make new Manager"))

	cmdName := "Test"
	m.RegisterService(cmdName, sysinfoService)

	// The agent calls .Start().
	err = m.Start()
	t.Assert(err, IsNil)

	// Its status should be "Running".
	status := m.Status()
	t.Check(status[sysinfo.SERVICE_NAME], Equals, "Running")

	// Can't start manager twice.
	err = m.Start()
	t.Check(err, FitsTypeOf, pct.ServiceIsRunningError{})

	// Test known cmd
	cmd := &proto.Cmd{
		Service: sysinfo.SERVICE_NAME,
		Cmd:     cmdName,
	}
	gotReply := m.Handle(cmd)
	t.Assert(gotReply, NotNil)
	t.Assert(gotReply.Error, Equals, "")

	// Test unknown cmd
	cmd = &proto.Cmd{
		Service: sysinfo.SERVICE_NAME,
		Cmd:     "Unknown",
	}
	gotReply = m.Handle(cmd)
	t.Assert(gotReply, NotNil)
	t.Assert(gotReply.Error, Equals, fmt.Sprintf("Unknown command: %s", cmd.Cmd))

	// You can't stop this service
	err = m.Stop()
	t.Check(err, IsNil)
	status = m.Status()
	t.Check(status[sysinfo.SERVICE_NAME], Equals, "Running")
}
