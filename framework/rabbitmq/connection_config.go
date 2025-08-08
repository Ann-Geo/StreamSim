package rabbitmq

import (
	"dstream-sim/arguments"
	"dstream-sim/helpers"
	"encoding/json"
	"fmt"
	"os"
)

type ConnectionConfig struct {
	Toolkit        string         `json:"toolkit"`
	TlsEncryption  string         `json:"tlsEncryption"`
	Hostname       string         `json:"hostname"`
	Port           int            `json:"port"`
	AmqpsUrl       string         `json:"amqpsUrl"`
	Authentication Authentication `json:"authentication"`
}

type Authentication struct {
	Method      string      `json:"method"`
	Credentials Credentials `json:"credentials"`
}

type Credentials struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

func (c *ConnectionConfig) ParseConnectionConfig(simIns *arguments.SimulatorIns) error {
	bytes, err := os.ReadFile(*simIns.FrameworkConfigFile)
	if err != nil {
		helpers.ErrorLogger.Log(helpers.ERROR, "Error reading connection config file: %v\n", err)
		return err
	}

	err = json.Unmarshal(bytes, &c)
	if err != nil {
		helpers.ErrorLogger.Log(helpers.ERROR, "Unable to unmarshal connection config file: %v\n", err)
		return err
	}

	if c.Toolkit == "" {
		err := fmt.Errorf("toolkit is required")
		helpers.ErrorLogger.Log(helpers.ERROR, "Unable to get toolkit: %v\n", err)
		return err
	}
	if c.TlsEncryption == "" {
		err := fmt.Errorf("tls encryption option is required")
		helpers.ErrorLogger.Log(helpers.ERROR, "Unable to get Tls encryption option: %v\n", err)
		return err
	}
	if c.Hostname == "" {
		err := fmt.Errorf("hostname is required")
		helpers.ErrorLogger.Log(helpers.ERROR, "Unable to get hostname: %v\n", err)
		return err
	}
	if c.Port == 0 {
		err := fmt.Errorf("port is required and must be a non-zero integer")
		helpers.ErrorLogger.Log(helpers.ERROR, "Unable to get port: %v\n", err)
		return err
	}
	if c.AmqpsUrl == "" {
		err := fmt.Errorf("amqpsUrl is required and must be a Url")
		helpers.ErrorLogger.Log(helpers.ERROR, "Unable to get amqpsUrl: %v\n", err)
		return err
	}
	if c.Authentication.Method != "username_password" {
		err := fmt.Errorf("unsupported authentication method: %s", c.Authentication.Method)
		helpers.ErrorLogger.Log(helpers.ERROR, "Unable to get authentication method: %v\n", err)
		return err
	}
	if c.Authentication.Credentials.Username == "" || c.Authentication.Credentials.Password == "" {
		err := fmt.Errorf("both username and password are required for authentication")
		helpers.ErrorLogger.Log(helpers.ERROR, "Unable to get username or password: %v\n", err)
		return err
	}

	return nil
}
func (c *ConnectionConfig) GetAmqpsUrl() string {

	return c.AmqpsUrl
}
func (c *ConnectionConfig) GetTlsEncryption() string {

	return c.TlsEncryption
}

func (c *ConnectionConfig) GetToolkit() string {

	return c.Toolkit
}
