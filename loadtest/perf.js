import http from 'k6/http';
import { check } from 'k6';

const vus = Number(__ENV.VUS || 100);
const duration = __ENV.DURATION || '30s';
const baseURL = __ENV.BASE_URL || 'http://127.0.0.1:8080';

export const options = {
  scenarios: {
    create_tasks: {
      executor: 'constant-vus',
      vus,
      duration,
      gracefulStop: '5s',
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.01'],
  },
};

export default function () {
  const payload = JSON.stringify({
    name: `perf-${__VU}-${__ITER}`,
    type: 'compute',
    payload: 'performance-test',
  });

  const res = http.post(`${baseURL}/api/v1/tasks`, payload, {
    headers: { 'Content-Type': 'application/json' },
    tags: { endpoint: 'create-task' },
  });

  check(res, {
    'task created': (r) => r.status === 201,
  });
}
