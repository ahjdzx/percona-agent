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

package installer

import (
	"fmt"
	"github.com/mewpkg/gopass"
	"github.com/percona/percona-agent/mysql"
	"log"
	"os/exec"
	"os/user"
	"path/filepath"
	"regexp"
	"strings"
)

func MakeGrant(dsn mysql.DSN, user string, pass string) []string {
	host := "%"
	if dsn.Socket != "" || dsn.Hostname == "localhost" {
		host = "localhost"
	} else if dsn.Hostname == "127.0.0.1" {
		host = "127.0.0.1"
	}
	grants := []string{
		fmt.Sprintf("GRANT SUPER, PROCESS, USAGE, SELECT ON *.* TO '%s'@'%s' IDENTIFIED BY '%s'", user, host, pass),
		fmt.Sprintf("GRANT UPDATE, DELETE, DROP ON performance_schema.* TO '%s'@'%s' IDENTIFIED BY '%s'", user, host, pass),
	}
	return grants
}

func (i *Installer) getAgentDSN() (dsn mysql.DSN, err error) {
	if i.flags.Bool["create-mysql-user"] {
		// Connect as root, create percona-agent MySQL user.
		dsn, err = i.createNewMySQLUser()
		if err != nil {
			fmt.Println(err)
			return dsn, fmt.Errorf("Failed to create MySQL user for agent")
		}
		fmt.Printf("Created MySQL user: %s\n", dsn.StringWithSuffixes())
	} else {
		if i.flags.Bool["interactive"] {
			// Prompt for existing percona-agent MySQL user.
			dsn, err = i.useExistingMySQLUser()
			if err != nil {
				fmt.Println(err)
				return dsn, fmt.Errorf("Failed to get MySQL user for agent")
			}
			fmt.Printf("Using MySQL user: %s\n", dsn.StringWithSuffixes())
		} else {
			// Non-MySQL install (e.g. only system metrics).
			fmt.Println("Skip creating MySQL user (-create-mysql-user=false)")
			return dsn, nil
		}
	}
	return dsn, nil
}

func (i *Installer) createNewMySQLUser() (dsn mysql.DSN, err error) {
	// Auto-detect the root MySQL user connection options.
	superUserDSN := i.defaultDSN
	if i.flags.Bool["auto-detect-mysql"] {
		if err := i.autodetectDSN(&superUserDSN); err != nil {
			if i.flags.Bool["debug"] {
				log.Println(err)
			}
		}
	}
	fmt.Printf("MySQL root DSN: %s\n", superUserDSN)

	// Try to connect as root automatically.  If this fails and interacive is true,
	// start prompting user to enter valid root MySQL connection info.
	if err = i.verifyMySQLConnection(superUserDSN); err != nil {
		fmt.Printf("Error connecting to MySQL %s: %s\n", superUserDSN, err)
		if i.flags.Bool["interactive"] {
			if again, err := i.term.PromptBool("Try again?", "Y"); err != nil {
				return superUserDSN, err
			} else if !again {
				return superUserDSN, fmt.Errorf("Failed to connect to MySQL")
			}
			fmt.Println("Specify a root/super MySQL user to create a user for the agent")
			if err := i.getDSNFromUser(&superUserDSN); err != nil {
				return dsn, err
			}
		} else {
			// Can't auto-detect MySQL root user and not interactive, fail.
			return dsn, err
		}
	}

	dsn, err = i.createMySQLUser(superUserDSN)
	if err != nil {
		return dsn, err
	}

	// Verify new DSN
	if err := i.verifyMySQLConnection(dsn); err != nil {
		return dsn, err
	}

	return dsn, nil
}

func (i *Installer) useExistingMySQLUser() (mysql.DSN, error) {
	userDSN := i.defaultDSN
	userDSN.Username = "percona-agent"
	userDSN.Password = ""
	if i.flags.Bool["auto-detect-mysql"] {
		if err := i.autodetectDSN(&userDSN); err != nil {
			if i.flags.Bool["debug"] {
				log.Println(err)
			}
		}
	}
	for {
		// Let user specify the MySQL account to use for the agent.
		fmt.Println("Specify the existing MySQL user to use for the agent")
		if err := i.getDSNFromUser(&userDSN); err != nil {
			return userDSN, nil
		}

		// Verify DSN provided by user
		if err := i.verifyMySQLConnection(userDSN); err != nil {
			fmt.Printf("Error connecting to MySQL %s: %s\n", userDSN, err)
			if i.flags.Bool["interactive"] {
				if again, err := i.term.PromptBool("Try again?", "Y"); err != nil {
					return userDSN, err
				} else if !again {
					return userDSN, fmt.Errorf("Failed to connect to MySQL")
				}
				continue // again
			} else {
				// Can't auto-detect MySQL root user and not interactive, fail.
				return userDSN, err
			}
		}
		return userDSN, nil // success
	}
}

func (i *Installer) getDSNFromUser(dsn *mysql.DSN) error {
	// Ask for username
	username, err := i.term.PromptString("MySQL username", dsn.Username)
	if err != nil {
		return err
	}
	dsn.Username = username

	// Ask for password
	var password string
	if i.flags.Bool["plain-passwords"] {
		password, err = i.term.PromptString("MySQL password", "")
	} else {
		password, err = gopass.GetPass("MySQL password: ")
	}
	if err != nil {
		return err
	}
	dsn.Password = password

	// Ask for hostname / socket path
	hostname, err := i.term.PromptStringRequired(
		"MySQL host[:port] or socket file",
		dsn.To(),
	)
	if err != nil {
		return err
	}
	if filepath.IsAbs(hostname) {
		dsn.Socket = hostname
		dsn.Hostname = ""
	} else {
		f := strings.Split(hostname, ":")
		dsn.Hostname = f[0]
		if len(f) > 1 {
			dsn.Port = f[1]
		} else {
			dsn.Port = "3306"
		}
		dsn.Socket = ""
	}
	return nil
}

func (i *Installer) autodetectDSN(dsn *mysql.DSN) error {
	params := []string{}
	if i.flags.String["mysql-defaults-file"] != "" {
		params = append(params, "--defaults-file="+i.flags.String["mysql-defaults-file"])
	}
	// --print-defaults needs to be last param
	params = append(params, "--print-defaults")
	cmd := exec.Command("mysql", params...)
	byteOutput, err := cmd.Output()
	if err != nil {
		return err
	}
	output := string(byteOutput)
	if i.flags.Bool["debug"] {
		log.Println(output)
	}
	autoDSN := ParseMySQLDefaults(output)
	if i.flags.Bool["debug"] {
		log.Printf("autoDSN: %#v\n", autoDSN)
	}
	// Fill in the given DSN with auto-detected options.
	if dsn.Username == "" {
		dsn.Username = autoDSN.Username
	}
	if dsn.Password == "" {
		dsn.Password = autoDSN.Password
	}
	if dsn.Hostname == "" {
		dsn.Hostname = autoDSN.Hostname
	}
	if dsn.Port == "" {
		dsn.Port = autoDSN.Port
	}
	if dsn.Socket == "" {
		dsn.Socket = autoDSN.Socket
	}
	if dsn.Username == "" {
		user, err := user.Current()
		if err == nil {
			dsn.Username = user.Username
		}
	}
	if i.flags.Bool["debug"] {
		log.Printf("dsn: %#v\n", dsn)
	}
	return nil
}

func ParseMySQLDefaults(output string) *mysql.DSN {
	var re *regexp.Regexp
	var result []string // Result of FindStringSubmatch
	dsn := &mysql.DSN{}

	// Note: Since output of mysql --print-defaults
	//       doesn't use quotation marks for values
	//       then we use "space" as a separator
	//       this implies that we are unable to properly detect
	//       e.g. passwords with spaces
	re = regexp.MustCompile("--user=([^ ]+)")
	result = re.FindStringSubmatch(output)
	if result != nil {
		dsn.Username = result[1]
	}

	re = regexp.MustCompile("--password=([^ ]+)")
	result = re.FindStringSubmatch(output)
	if result != nil {
		dsn.Password = result[1]
	}

	re = regexp.MustCompile("--socket=([^ ]+)")
	result = re.FindStringSubmatch(output)
	if result != nil {
		dsn.Socket = result[1]
	}

	if dsn.Socket == "" {
		re = regexp.MustCompile("--host=([^ ]+)")
		result = re.FindStringSubmatch(output)
		if result != nil {
			dsn.Hostname = result[1]
		}

		re = regexp.MustCompile("--port=([^ ]+)")
		result = re.FindStringSubmatch(output)
		if result != nil {
			dsn.Port = result[1]
		}
	}

	// Hostname always defaults to localhost.  If localhost means 127.0.0.1 or socket
	// is handled by mysql/DSN.DSN().
	if dsn.Hostname == "" && dsn.Socket == "" {
		dsn.Hostname = "localhost"
	}

	return dsn
}

func (i *Installer) verifyMySQLConnection(dsn mysql.DSN) (err error) {
	dsnString, err := dsn.DSN()
	if err != nil {
		return err
	}
	if i.flags.Bool["debug"] {
		log.Printf("verifyMySQLConnection: %#v %s\n", dsn, dsnString)
	}
	conn := mysql.NewConnection(dsnString)
	if err := conn.Connect(1); err != nil {
		return err
	}
	conn.Close()
	return nil
}