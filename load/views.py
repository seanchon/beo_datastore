from rest_framework import views
from rest_framework.exceptions import UnsupportedMediaType
from rest_framework.parsers import MultiPartParser
from rest_framework.response import Response

from beo_datastore.libs.api.viewsets import ListRetrieveDestroyViewSet

from reference.reference_model.models import OriginFile, MeterIntervalFrame

from .serializers import MeterIntervalFrameSerializer, OriginFileSerializer


class OriginFileView(views.APIView):
    """
    Files containing customer Meter data. A "file" must be provided in the
    payload.
    """

    parser_classes = [MultiPartParser]

    def post(self, request, filename, format=None):
        f = request.data["file"]
        # TODO: add additional file validation
        if f.content_type == "text/csv":
            origin_file = OriginFile()
            origin_file.file.save(filename, f, save=True)
            origin_file.owners.add(request.user)
            # TODO: async ingest meters
            # TODO: return link to asset in response
            return Response(status=204)
        else:
            raise UnsupportedMediaType("Upload must be a .csv file.")


class OriginFileViewSet(ListRetrieveDestroyViewSet):
    """
    Files containing customer Meter data.
    """

    queryset = OriginFile.objects.all()
    serializer_class = OriginFileSerializer


class MeterViewSet(ListRetrieveDestroyViewSet):
    """
    Meters and/or ReferenceBuildings with associated interval data.
    """

    queryset = MeterIntervalFrame.objects.all()
    serializer_class = MeterIntervalFrameSerializer
