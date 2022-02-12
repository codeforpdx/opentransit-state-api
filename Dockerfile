FROM node:8.6.0

# Create app directory
WORKDIR /usr/src/app

# Install dependencies
COPY package.json package-lock.json ./
RUN npm install

# Bundle app source
COPY . .

CMD [ "npm", "start" ]

# Build
# docker build -t opentransit-state-api .

# Run
# docker run -p 4000:4000 opentransit-state-api:latest
