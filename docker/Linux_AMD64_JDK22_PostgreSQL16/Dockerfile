FROM rockylinux:9

ARG github_pat
ENV GITHUB_PAT=$github_pat
ENV JAVA_HOME=/opt/jdk
ENV PATH=/opt/jdk/bin:/opt/maven/bin:$PATH

# Install various dependencies, except maven and java, because the yum
# versions are old and will bring in tons of other stuff.
RUN yum install -y \
    git \
    wget

# Install a JDK & Maven
RUN wget -q https://download.java.net/java/GA/jdk22/830ec9fcccef480bb3e73fb7ecafe059/36/GPL/openjdk-22_linux-x64_bin.tar.gz \
    && tar xfz openjdk-22_linux-x64_bin.tar.gz \
    && mv jdk-22 $JAVA_HOME \
    && wget -q https://dlcdn.apache.org/maven/maven-3/3.9.6/binaries/apache-maven-3.9.6-bin.tar.gz \
    && tar xfz apache-maven-3.9.6-bin.tar.gz \
    && mv apache-maven-3.9.6 /opt/maven

ARG user=galia
ARG home=/home/$user
RUN adduser --home $home $user && chown -R $user $home
USER $user
WORKDIR $home

# Copy plugin code
COPY --chown=galia . .

RUN mv docker/Linux_AMD64_JDK22_PostgreSQL16/config.yml .

ENTRYPOINT exec docker/Linux_AMD64_JDK22_PostgreSQL16/test.sh $GITHUB_PAT