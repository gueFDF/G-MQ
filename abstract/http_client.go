package abstract

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// 定制创建一个http连接池
func NewDeadlineTransport(connectTimeout time.Duration, responseTimeout time.Duration) *http.Transport {
	transport := &http.Transport{
		//不加密的tcp连接
		DialContext: (&net.Dialer{
			Timeout:   connectTimeout, //超时时长
			KeepAlive: 30 * time.Second,
			DualStack: true,
		}).DialContext,
		ResponseHeaderTimeout: responseTimeout,  //连接成功后等待响应的超时时长，设置为0表示无限制
		IdleConnTimeout:       90 * time.Second, //连接空闲的最长时间，超时自动断开（0表示没有限制）
		TLSHandshakeTimeout:   10 * time.Second, //tls的超时时长
		MaxIdleConns:          100,              //最大的空闲连接数(跨主机)
		MaxIdleConnsPerHost:   2,                //每个主机的最大空闲连接数
	}
	return transport
}

// httpClient
type HttpClient struct {
	c *http.Client
}

// 创建
func NewClient(tlsConfig *tls.Config, connectTimeout time.Duration, resquestTimeout time.Duration) *HttpClient {
	transport := NewDeadlineTransport(connectTimeout, resquestTimeout)
	transport.TLSClientConfig = tlsConfig
	return &HttpClient{
		c: &http.Client{
			Transport: transport,
			Timeout:   resquestTimeout,
		},
	}
}

// Get请求,v是传出参数
func (c *HttpClient) Get(endpoint string, v interface{}) error {
retry:
	req, err := http.NewRequest("GET", endpoint, nil)
	if err != nil {
		return err
	}
	req.Header.Add("Accept", "application/json")
	resp, err := c.c.Do(req)
	if err != nil {
		return err
	}
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		return err
	}
	//这一步，请求失败的原因可能是需要使用https,所以根据httpsEndpoint
	//转换为https的endpoint
	if resp.StatusCode != 200 {
		//403:代表客户端错误，指的是服务器端有能力处理该请求，但是拒绝授权访问。
		if resp.StatusCode == 403 && !strings.HasPrefix(endpoint, "https") {
			endpoint, err = httpsEndpoint(endpoint, body)
			if err != nil {
				return err
			}
			goto retry
		}
		return fmt.Errorf("got response %s %q", resp.Status, body)
	}

	//将body解析到body中
	err = json.Unmarshal(body, &v)
	if err != nil {
		return err
	}
	return nil
}

func (c *HttpClient) POST(endpoint string) error {
retry:
	req, err := http.NewRequest("POST", endpoint, nil)
	if err != nil {
		return err
	}
	req.Header.Add("Accept", "application/json")
	resp, err := c.c.Do(req)
	if err != nil {
		return err
	}
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		return err
	}
	//这一步，请求失败的原因可能是需要使用https,所以根据httpsEndpoint
	//转换为https的endpoint
	if resp.StatusCode != 200 {
		//403:代表客户端错误，指的是服务器端有能力处理该请求，但是拒绝授权访问。
		if resp.StatusCode == 403 && !strings.HasPrefix(endpoint, "https") {
			endpoint, err = httpsEndpoint(endpoint, body)
			if err != nil {
				return err
			}
			goto retry
		}
		return fmt.Errorf("got response %s %q", resp.Status, body)
	}

	return nil
}

// 处理url,将http请求，变成https
func httpsEndpoint(endpoint string, body []byte) (string, error) {
	var forbiddenResp struct {
		HTTPSPort int `json:"https_port"`
	}
	err := json.Unmarshal(body, &forbiddenResp)
	if err != nil {
		return "", err
	}

	u, err := url.Parse(endpoint)
	if err != nil {
		return "", err
	}
	host, _, err := net.SplitHostPort(u.Host)
	u.Scheme = "https"
	u.Host = net.JoinHostPort(host, strconv.Itoa(forbiddenResp.HTTPSPort))
	return u.String(), nil
}
