import http from 'k6/http';
import { check } from 'k6';

export const options = {
  scenarios: {
    create_tasks: {
      executor: 'constant-vus',
      vus: 50,
      duration: '30s',
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.01'],
    http_req_duration: ['p(95)<300'],
  },
};

const baseURL = __ENV.BASE_URL || 'http://127.0.0.1:8080';

export default function () {
  const payload = JSON.stringify({
    name: `load-${__VU}-${__ITER}`,
    type: 'compute',
    payload: 'load-test',
  });
  const res = http.post(`${baseURL}/api/v1/tasks`, payload, {
    headers: { 'Content-Type': 'application/json' },
  });
  check(res, { 'created or throttled': (r) => r.status === 201 || r.status === 429 });
}
