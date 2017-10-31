package main

import (
	"fmt"
	"io/ioutil"
)

func install(c *cluster, args []string) error {
	for _, arg := range args {
		switch arg {
		case "cassandra":
			cmd := `
echo "deb http://www.apache.org/dist/cassandra/debian 311x main" | \
  sudo tee -a /etc/apt/sources.list.d/cassandra.sources.list;
curl https://www.apache.org/dist/cassandra/KEYS | sudo apt-key add -;
sudo apt-get update;
sudo apt-get install -y cassandra;
sudo service cassandra stop;
`
			if err := c.run(ioutil.Discard, c.nodes, "installing cassandra", cmd); err != nil {
				return err
			}

		case "mongodb":
			return fmt.Errorf("TODO(peter): unimplemented: mongodb")

		case "postgres":
			return fmt.Errorf("TODO(peter): unimplemented: postgres")

		case "tools":
			cmd := `
sudo apt-get install -y \
  fio \
  iftop \
  iotop \
  sysstat \
  linux-tools-common \
  linux-tools-4.13.0-16-generic \
  linux-cloud-tools-4.13.0-16-generic;
`
			if err := c.run(ioutil.Discard, c.nodes, "installing tools", cmd); err != nil {
				return err
			}
		}
	}
	return nil
}
